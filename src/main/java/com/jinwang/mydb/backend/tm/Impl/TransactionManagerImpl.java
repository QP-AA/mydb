package com.jinwang.mydb.backend.tm.Impl;

import com.jinwang.mydb.backend.tm.TransactionManager;
import com.jinwang.mydb.common.Error;
import com.jinwang.mydb.utils.Panic;
import com.jinwang.mydb.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author jinwang
 * @Date 2023/12/1 15:22
 * @Version 1.0 （版本号）
 */
public class TransactionManagerImpl implements TransactionManager {

    public static  final int LEN_XID_HEADER_LENGTH = 8;  // 文件头长度
    private static final int XID_FIELD_SIZE = 1;  // 每个事务占用长度
    private static final byte FIELD_TRAN_ACTIVE = 0;  // 正在执行
    private static final byte FIELD_TRAN_COMMITTED = 1;  // 已提交
    private static final byte FIELD_TRAN_ABORTED = 2;  // 已取消
    private static final long SUPER_XID = 0;  // 超级事务
    public static final String XID_SUFFIX = ".xid";  // 文件后缀

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;


    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());  // 将长度为8的字节数组解析为long类型
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据xid获得事务在文件中对应位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid状态
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        }catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);  // 强制将文件通道中数据写入磁盘
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 更新xidCounter和head
     */
    private void incrXIDCCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        }catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);  // 强制将文件通道中数据写入磁盘
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开启事务并返回XID
     * @return xid
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检查指定xid是否为所需状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        // 读取文件中xid的状态
        try {
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
