/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.store.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.seata.common.exception.StoreException;
import io.seata.common.loader.LoadLevel;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.CollectionUtils;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionCondition;
import io.seata.server.session.SessionManager;
import io.seata.server.store.AbstractTransactionStoreManager;
import io.seata.server.store.FlushDiskMode;
import io.seata.server.store.ReloadableStore;
import io.seata.server.store.SessionStorable;
import io.seata.server.store.StoreConfig;
import io.seata.server.store.TransactionStoreManager;
import io.seata.server.store.TransactionWriteStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type File transaction store manager.
 * 通过文件保存事务
 *
 * @author jimin.jm @alibaba-inc.com
 */
@LoadLevel(name = "file")
public class FileTransactionStoreManager extends AbstractTransactionStoreManager
    implements TransactionStoreManager, ReloadableStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileTransactionStoreManager.class);

    private static final int MAX_THREAD_WRITE = 1;

    private ExecutorService fileWriteExecutor;

    private volatile boolean stopping = false;

    private static final int MAX_SHUTDOWN_RETRY = 3;

    private static final int SHUTDOWN_CHECK_INTERNAL = 1 * 1000;

    private static final int MAX_WRITE_RETRY = 5;

    private static final String HIS_DATA_FILENAME_POSTFIX = ".1";

    // 文件事务数量
    private static final AtomicLong FILE_TRX_NUM = new AtomicLong(0);

    // 刷新到磁盘的事务数量
    private static final AtomicLong FILE_FLUSH_NUM = new AtomicLong(0);

    private static final int MARK_SIZE = 4;

    private static final int MAX_WAIT_TIME_MILLS = 2 * 1000;

    private static final int MAX_FLUSH_TIME_MILLS = 2 * 1000;

    private static final int MAX_FLUSH_NUM = 10;

    private static int PER_FILE_BLOCK_SIZE = 65535 * 8;

    private static long MAX_TRX_TIMEOUT_MILLS = 30 * 60 * 1000;

    // 文件上次更新的时间戳
    private static volatile long trxStartTimeMills = System.currentTimeMillis();

    private File currDataFile;

    // 当前写入的文件
    private RandomAccessFile currRaf;

    // 文件channel
    private FileChannel currFileChannel;

    private long recoverCurrOffset = 0;

    private long recoverHisOffset = 0;

    // session管理器
    private SessionManager sessionManager;

    private String currFullFileName;

    private String hisFullFileName;

    // 执行写入的后台任务
    private WriteDataFileRunnable writeDataFileRunnable;

    private ReentrantLock writeSessionLock = new ReentrantLock();

    private volatile long lastModifiedTime;

    private static final int MAX_WRITE_BUFFER_SIZE = StoreConfig.getFileWriteBufferCacheSize();

    // 写入缓冲区
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(MAX_WRITE_BUFFER_SIZE);

    private static final FlushDiskMode FLUSH_DISK_MODE = StoreConfig.getFlushDiskMode();

    private static final int MAX_WAIT_FOR_FLUSH_TIME_MILLS = 2 * 1000;

    private static final int INT_BYTE_SIZE = 4;

    /**
     * Instantiates a new File transaction store manager.
     *
     * @param fullFileName   the dir path
     * @param sessionManager the session manager
     * @throws IOException the io exception
     */
    public FileTransactionStoreManager(String fullFileName, SessionManager sessionManager) throws IOException {
        // 初始化文件通道
        initFile(fullFileName);
        fileWriteExecutor = new ThreadPoolExecutor(MAX_THREAD_WRITE, MAX_THREAD_WRITE, Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("fileTransactionStore", MAX_THREAD_WRITE, true));
        // 提交写入人任务
        writeDataFileRunnable = new WriteDataFileRunnable();
        fileWriteExecutor.submit(writeDataFileRunnable);
        this.sessionManager = sessionManager;
    }

    // 初始化写入文件和channel
    private void initFile(String fullFileName) throws IOException {
        this.currFullFileName = fullFileName;
        this.hisFullFileName = fullFileName + HIS_DATA_FILENAME_POSTFIX;
        try {
            currDataFile = new File(currFullFileName);
            // 不存在则新建父级目录和文件
            if (!currDataFile.exists()) {
                //create parent dir first
                if (currDataFile.getParentFile() != null && !currDataFile.getParentFile().exists()) {
                    currDataFile.getParentFile().mkdirs();
                }
                currDataFile.createNewFile();
                trxStartTimeMills = System.currentTimeMillis();
            } else {
                trxStartTimeMills = currDataFile.lastModified();
            }
            lastModifiedTime = System.currentTimeMillis();
            // 创建channel并在末尾写入
            currRaf = new RandomAccessFile(currDataFile, "rw");
            currRaf.seek(currDataFile.length());
            currFileChannel = currRaf.getChannel();
        } catch (IOException exx) {
            LOGGER.error("init file error," + exx.getMessage());
            throw exx;
        }
    }

    @Override
    public boolean writeSession(LogOperation logOperation, SessionStorable session) {
        writeSessionLock.lock();
        long curFileTrxNum;
        try {
            // 写入文件
            if (!writeDataFile(new TransactionWriteStore(session, logOperation).encode()/*序列化session*/)) {
                return false;
            }
            // 更新写入时间戳
            lastModifiedTime = System.currentTimeMillis();
            curFileTrxNum = FILE_TRX_NUM.incrementAndGet();
            if (curFileTrxNum % PER_FILE_BLOCK_SIZE == 0 &&
                (System.currentTimeMillis() - trxStartTimeMills) > MAX_TRX_TIMEOUT_MILLS) {
                // 文件已满，生成历史文件并新建写入文件
                return saveHistory();
            }
        } catch (Exception exx) {
            LOGGER.error("writeSession error," + exx.getMessage());
            return false;
        } finally {
            writeSessionLock.unlock();
        }
        // 刷盘
        flushDisk(curFileTrxNum, currFileChannel);
        return true;
    }

    private void flushDisk(long curFileNum, FileChannel currFileChannel) {

        if (FLUSH_DISK_MODE == FlushDiskMode.SYNC_MODEL) {
            SyncFlushRequest syncFlushRequest = new SyncFlushRequest(curFileNum, currFileChannel);
            writeDataFileRunnable.putRequest(syncFlushRequest);
            syncFlushRequest.waitForFlush(MAX_WAIT_FOR_FLUSH_TIME_MILLS);
        } else {
            writeDataFileRunnable.putRequest(new AsyncFlushRequest(curFileNum, currFileChannel));
        }
    }

    /**
     * get all overTimeSessionStorables
     * merge write file
     * 写入所以超时的session
     * 当前文件改名为历史文件，并新建写入文件
     *
     * @throws IOException
     */
    private boolean saveHistory() throws IOException {
        boolean result;
        try {
            // 将超时的session写入文件
            result = findTimeoutAndSave();
            // 提交关闭文件操作
            writeDataFileRunnable.putRequest(new CloseFileRequest(currFileChannel, currRaf));
            // 将当前文件改名为历史文件
            Files.move(currDataFile.toPath(), new File(hisFullFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exx) {
            LOGGER.error("save history data file error," + exx.getMessage());
            result = false;
        } finally {
            // 新建写入文件
            initFile(currFullFileName);
        }
        return result;
    }

    // 查询出超时的session并保存
    private boolean findTimeoutAndSave() throws IOException {
        // 查询超时的session
        List<GlobalSession> globalSessionsOverMaxTimeout =
            sessionManager.findGlobalSessions(new SessionCondition(MAX_TRX_TIMEOUT_MILLS));
        if (CollectionUtils.isEmpty(globalSessionsOverMaxTimeout)) {
            return true;
        }
        List<byte[]> listBytes = new ArrayList<>();
        int totalSize = 0;
        // 1. find all data and merge。序列化session
        for (GlobalSession globalSession : globalSessionsOverMaxTimeout) {
            // 序列化session
            TransactionWriteStore globalWriteStore = new TransactionWriteStore(globalSession, LogOperation.GLOBAL_ADD);
            byte[] data = globalWriteStore.encode();
            listBytes.add(data);
            totalSize += data.length + INT_BYTE_SIZE;
            // 序列化包含的BranchSession
            List<BranchSession> branchSessIonsOverMaXTimeout = globalSession.getSortedBranches();
            if (null != branchSessIonsOverMaXTimeout) {
                for (BranchSession branchSession : branchSessIonsOverMaXTimeout) {
                    TransactionWriteStore branchWriteStore =
                        new TransactionWriteStore(branchSession, LogOperation.BRANCH_ADD);
                    data = branchWriteStore.encode();
                    listBytes.add(data);
                    totalSize += data.length + INT_BYTE_SIZE;
                }
            }
        }
        // 2. batch write。批量写入
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(totalSize);
        for (byte[] bytes : listBytes) {
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
        }
        if (writeDataFileByBuffer(byteBuffer)) {
            currFileChannel.force(false);
            return true;
        }
        return false;
    }

    @Override
    public GlobalSession readSession(String xid) {
        throw new StoreException("unsupport for read from file, xid:" + xid);
    }

    @Override
    public List<GlobalSession> readSession(SessionCondition sessionCondition) {
        throw new StoreException("unsupport for read from file");
    }

    @Override
    public void shutdown() {
        if (null != fileWriteExecutor) {
            fileWriteExecutor.shutdown();
            stopping = true;
            int retry = 0;
            while (!fileWriteExecutor.isTerminated() && retry < MAX_SHUTDOWN_RETRY) {
                ++retry;
                try {
                    Thread.sleep(SHUTDOWN_CHECK_INTERNAL);
                } catch (InterruptedException ignore) {
                }
            }
            if (retry >= MAX_SHUTDOWN_RETRY) {
                fileWriteExecutor.shutdownNow();
            }
        }
        try {
            currFileChannel.force(true);
        } catch (IOException e) {
            LOGGER.error("filechannel force error", e);
        }
        closeFile(currRaf);
    }

    @Override
    public List<TransactionWriteStore> readWriteStore(int readSize, boolean isHistory) {
        File file = null;
        long currentOffset = 0;
        if (isHistory) {
            file = new File(hisFullFileName);
            currentOffset = recoverHisOffset;
        } else {
            file = new File(currFullFileName);
            currentOffset = recoverCurrOffset;
        }
        // 读取文件记录
        if (file.exists()) {
            return parseDataFile(file, readSize, currentOffset);
        }
        return null;
    }

    @Override
    public boolean hasRemaining(boolean isHistory) {
        File file = null;
        RandomAccessFile raf = null;
        long currentOffset = 0;
        if (isHistory) {
            file = new File(hisFullFileName);
            currentOffset = recoverHisOffset;
        } else {
            file = new File(currFullFileName);
            currentOffset = recoverCurrOffset;
        }
        try {
            raf = new RandomAccessFile(file, "r");
            return currentOffset < raf.length();

        } catch (IOException ignore) {
        } finally {
            closeFile(raf);
        }
        return false;
    }

    // 指定offset，读取文件
    private List<TransactionWriteStore> parseDataFile(File file, int readSize, long currentOffset) {
        List<TransactionWriteStore> transactionWriteStores = new ArrayList<>(readSize);
        RandomAccessFile raf = null;
        FileChannel fileChannel = null;
        try {
            raf = new RandomAccessFile(file, "r");
            raf.seek(currentOffset);
            fileChannel = raf.getChannel();
            fileChannel.position(currentOffset);
            long size = raf.length();
            ByteBuffer buffSize = ByteBuffer.allocate(MARK_SIZE);
            while (fileChannel.position() < size) {
                try {
                    buffSize.clear();
                    // 读取数据长度
                    int avilReadSize = fileChannel.read(buffSize);
                    if (avilReadSize != MARK_SIZE) {
                        break;
                    }
                    buffSize.flip();
                    // 根据长度读取正文
                    int bodySize = buffSize.getInt();
                    byte[] byBody = new byte[bodySize];
                    ByteBuffer buffBody = ByteBuffer.wrap(byBody);
                    avilReadSize = fileChannel.read(buffBody);
                    // 未读取完整
                    if (avilReadSize != bodySize) {
                        break;
                    }
                    // 反序列化
                    TransactionWriteStore writeStore = new TransactionWriteStore();
                    writeStore.decode(byBody);
                    // 添加到结果
                    transactionWriteStores.add(writeStore);
                    // 读取到足够的数据，完成
                    if (transactionWriteStores.size() == readSize) {
                        break;
                    }
                } catch (Exception ex) {
                    LOGGER.error("decode data file error:" + ex.getMessage());
                    break;
                }
            }
            return transactionWriteStores;
        } catch (IOException exx) {
            LOGGER.error("parse data file error:" + exx.getMessage() + ",file:" + file.getName());
            return null;
        } finally {
            try {
                if (null != fileChannel) {
                    if (isHisFile(file)) {
                        recoverHisOffset = fileChannel.position();
                    } else {
                        recoverCurrOffset = fileChannel.position();
                    }
                }
                closeFile(raf);
            } catch (IOException exx) {
                LOGGER.error("file close error," + exx.getMessage());
            }
        }

    }

    private boolean isHisFile(File file) {

        return file.getName().endsWith(HIS_DATA_FILENAME_POSTFIX);
    }

    private void closeFile(RandomAccessFile raf) {
        try {
            if (null != raf) {
                raf.close();
                raf = null;
            }
        } catch (IOException exx) {
            LOGGER.error("file close error," + exx.getMessage());
        }
    }

    // 写入session
    private boolean writeDataFile(byte[] bs) {
        if (bs == null) {
            return false;
        }
        ByteBuffer byteBuffer = null;

        if (bs.length > MAX_WRITE_BUFFER_SIZE) {
            //allocateNew
            byteBuffer = ByteBuffer.allocateDirect(bs.length);
        } else {
            byteBuffer = writeBuffer;
            //recycle
            byteBuffer.clear();
        }

        byteBuffer.putInt(bs.length);
        byteBuffer.put(bs);
        // 写入文件
        return writeDataFileByBuffer(byteBuffer);
    }

    // 将buffer写入文件
    private boolean writeDataFileByBuffer(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        for (int retry = 0; retry < MAX_WRITE_RETRY; retry++) {
            try {
                while (byteBuffer.hasRemaining()) {
                    currFileChannel.write(byteBuffer);
                }
                return true;
            } catch (IOException exx) {
                LOGGER.error("write data file error:" + exx.getMessage());
            }
        }
        LOGGER.error("write dataFile failed,retry more than :" + MAX_WRITE_RETRY);
        return false;
    }

    interface StoreRequest {

    }

    abstract class AbstractFlushRequest implements StoreRequest {
        private final long curFileTrxNum;

        private final FileChannel curFileChannel;

        protected AbstractFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            this.curFileTrxNum = curFileTrxNum;
            this.curFileChannel = curFileChannel;
        }

        public long getCurFileTrxNum() {
            return curFileTrxNum;
        }

        public FileChannel getCurFileChannel() {
            return curFileChannel;
        }
    }

    class SyncFlushRequest extends AbstractFlushRequest {

        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        public SyncFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            super(curFileTrxNum, curFileChannel);
        }

        public void wakeupCustomer() {
            this.countDownLatch.countDown();
        }

        public void waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }
    }

    class AsyncFlushRequest extends AbstractFlushRequest {

        public AsyncFlushRequest(long curFileTrxNum, FileChannel curFileChannel) {
            super(curFileTrxNum, curFileChannel);
        }

    }

    class CloseFileRequest implements StoreRequest {

        private FileChannel fileChannel;

        private RandomAccessFile file;

        public CloseFileRequest(FileChannel fileChannel, RandomAccessFile file) {
            this.fileChannel = fileChannel;
            this.file = file;
        }

        public FileChannel getFileChannel() {
            return fileChannel;
        }

        public RandomAccessFile getFile() {
            return file;
        }
    }

    /**
     * The type Write data file runnable.
     */
    class WriteDataFileRunnable implements Runnable {

        private LinkedBlockingQueue<StoreRequest> storeRequests = new LinkedBlockingQueue<>();

        public void putRequest(final StoreRequest request) {
            storeRequests.add(request);
        }

        @Override
        public void run() {
            while (!stopping) {
                try {
                    // 从队列拉取任务并处理
                    StoreRequest storeRequest = storeRequests.poll(MAX_WAIT_TIME_MILLS, TimeUnit.MILLISECONDS);
                    handleStoreRequest(storeRequest);
                } catch (Exception exx) {
                    LOGGER.error("write file error: {}", exx.getMessage(), exx);
                }
            }
            // 处理队列剩余的任务
            handleRestRequest();
        }

        /**
         * handle the rest requests when stopping is true
         */
        private void handleRestRequest() {
            int remainNums = storeRequests.size();
            for (int i = 0; i < remainNums; i++) {
                handleStoreRequest(storeRequests.poll());
            }
        }

        private void handleStoreRequest(StoreRequest storeRequest) {
            if (storeRequest == null) {
                // 传入空代表需要刷盘
                flushOnCondition(currFileChannel);
            }
            if (storeRequest instanceof SyncFlushRequest) {
                syncFlush((SyncFlushRequest)storeRequest);
            } else if (storeRequest instanceof AsyncFlushRequest) {
                async((AsyncFlushRequest)storeRequest);
            } else if (storeRequest instanceof CloseFileRequest) {
                closeAndFlush((CloseFileRequest)storeRequest);
            }
        }

        private void closeAndFlush(CloseFileRequest req) {
            // 刷盘
            long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
            flush(req.getFileChannel());
            FILE_FLUSH_NUM.addAndGet(diff);
            // 关闭文件
            closeFile(currRaf);
        }

        private void async(AsyncFlushRequest req) {
            if (req.getCurFileTrxNum() < FILE_FLUSH_NUM.get()) {
                flushOnCondition(req.getCurFileChannel());
            }
        }

        private void syncFlush(SyncFlushRequest req) {
            if (req.getCurFileTrxNum() < FILE_FLUSH_NUM.get()) {
                long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
                // 刷盘
                flush(req.getCurFileChannel());
                FILE_FLUSH_NUM.addAndGet(diff);
            }
            // notify
            req.wakeupCustomer();
        }

        private void flushOnCondition(FileChannel fileChannel) {
            if (FLUSH_DISK_MODE == FlushDiskMode.SYNC_MODEL) {
                return;
            }
            // 存在未写盘数据
            long diff = FILE_TRX_NUM.get() - FILE_FLUSH_NUM.get();
            if (diff == 0) {
                return;
            }
            if (diff % MAX_FLUSH_NUM == 0 ||
                System.currentTimeMillis() - lastModifiedTime > MAX_FLUSH_TIME_MILLS) {
                // 刷新到磁盘
                flush(fileChannel);
                FILE_FLUSH_NUM.addAndGet(diff);
            }
        }

        private void flush(FileChannel fileChannel) {
            try {
                fileChannel.force(false);
            } catch (IOException exx) {
                LOGGER.error("flush error:" + exx.getMessage());
            }
        }
    }
}
