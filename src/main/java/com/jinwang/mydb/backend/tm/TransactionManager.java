package com.jinwang.mydb.backend.tm;

import com.jinwang.mydb.backend.tm.Impl.TransactionManagerImpl;
import com.jinwang.mydb.utils.Panic;
import com.jinwang.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author jinwang
 * @Date 2023/12/1 15:21
 * @Version 1.0 （版本号）
 */
public interface TransactionManager {
    long begin();           // 开启事务
    void commit(long xid);  // 提交事务
    void abort(long xid);   // 回滚事务
    boolean isActive(long xid); // 查询一个事务的状态是否正在进行
    boolean isCommitted(long xid); // 查询一个事务状态是否为已提交
    boolean isAborted(long xid); // 查询一个事务状态是否为已取消
    void close();  // 关闭事务

    // 创建TransactionManagerImpl实例的静态工厂
    public static TransactionManagerImpl create(String path) {
        // TODO ?啥
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // 创建文件通道和随机访问文件对象
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 创建一个字节缓冲区ByteBuffer， 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);  // 将文件通道的位置设置在文件开头
            fc.write(buf);  // 将缓冲区的内容写入文件
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
