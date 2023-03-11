package com.yi.utils;

import com.jcraft.jsch.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SFTPUtils {
    private static final Log logger = LogFactory.getLog(SFTPUtils.class);

    private static Date last_push_date = null;

    private Session sshSession;

    private ChannelSftp channel;

    private static ThreadLocal<SFTPUtils> sftpLocal = new ThreadLocal<SFTPUtils>();

    public SFTPUtils(String host, int port, String username, String password) throws Exception {
        JSch jsch = new JSch();
        jsch.getSession(username, host, port);
        //根据用户名，密码，端口号获取session
        sshSession = jsch.getSession(username, host, port);
        sshSession.setPassword(password);
        //修改服务器/etc/ssh/sshd_config 中 GSSAPIAuthentication的值yes为no，解决用户不能远程登录
        sshSession.setConfig("userauth.gssapi-with-mic", "no");

        //为session对象设置properties,第一次访问服务器时不用输入yes
        sshSession.setConfig("StrictHostKeyChecking", "no");
        sshSession.connect();
        //获取sftp通道
        channel = (ChannelSftp)sshSession.openChannel("sftp");
        channel.connect();
        logger.info("连接ftp成功!");
    }

    /**
     * 是否已连接
     *
     * @return
     */
    private boolean isConnected() {
        return null != channel && channel.isConnected();
    }

    /**
     * 获取本地线程存储的sftp客户端
     *
     * @return
     * @throws Exception
     */
    public static SFTPUtils getSftpUtil(String host, int port, String username, String password) throws Exception {
        //获取本地线程
        SFTPUtils sftpUtil = sftpLocal.get();
        if (null == sftpUtil || !sftpUtil.isConnected()) {
            //将新连接防止本地线程，实现并发处理
            sftpLocal.set(new SFTPUtils(host, port, username, password));
        }
        return sftpLocal.get();
    }

    /**
     * 释放本地线程存储的sftp客户端
     */
    public static void release() {
        if (null != sftpLocal.get()) {
            sftpLocal.get().closeChannel();
            logger.info("关闭连接" + sftpLocal.get().sshSession);
            sftpLocal.set(null);

        }
    }

    /**
     * 关闭通道
     *
     * @throws Exception
     */
    public void closeChannel() {
        if (null != channel) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                logger.error("关闭SFTP通道发生异常:", e);
            }
        }
        if (null != sshSession) {
            try {
                sshSession.disconnect();
            } catch (Exception e) {
                logger.error("SFTP关闭 session异常:", e);
            }
        }

    }
    public void upload(String directory, String uploadFile, boolean isDel) throws Exception {
        try {       //执行列表展示ls 命令
            //channel.ls(directory);       //执行盘符切换cd 命令
            channel.cd(directory);
            List<File> files = getFiles(uploadFile, new ArrayList<File>());
            for (int i = 0; i < files.size(); i++) {
                File file = files.get(i);
                InputStream input = new BufferedInputStream(new FileInputStream(file));
                channel.put(input, file.getName());
                try {
                    if (input != null) input.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error(file.getName() + "关闭文件时.....异常!" + e.getMessage());
                }
//                if (file.exists()&&isDel) {
//                    boolean b = file.delete();
//                    logger.info(file.getName() + "文件上传完毕!删除标识:" + b);
//                }
            }
        }catch (Exception e) {
            logger.error("【子目录创建中】：",e);
            //创建子目录
            channel.mkdir(directory);
        }

    }
    public List<File> getFiles(String realpath, List<File> files) {
        File realFile = new File(realpath);
        if (realFile.isDirectory()) {
            File[] subfiles = realFile.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    if (null == last_push_date ) {
                        return true;
                    } else {
                        long modifyDate = file.lastModified();
                        return modifyDate > last_push_date.getTime();
                    }
                }
            });
            for (File file : subfiles) {
                if (file.isDirectory()) {
                    getFiles(file.getAbsolutePath(), files);
                } else {
                    files.add(file);
                }
                if (null == last_push_date) {
                    last_push_date = new Date(file.lastModified());
                } else {
                    long modifyDate = file.lastModified();
                    if (modifyDate > last_push_date.getTime()) {
                        last_push_date = new Date(modifyDate);
                    }
                }
            }
        }else {
            files.add(realFile);
        }
        return files;
    }

    public static long scpTo(String source, Session session, String destination) {
        FileInputStream fileInputStream = null;
        try {
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            OutputStream out = channel.getOutputStream();
            InputStream in = channel.getInputStream();
            boolean ptimestamp = false;
            String command = "scp";
            if (ptimestamp) {
                command += " -p";
            }
            command += " -t " + destination;
            channel.setCommand(command);
            channel.connect();

            File _lfile = new File(source);
            if (ptimestamp) {
                command = "T " + (_lfile.lastModified() / 1000) + " 0";
                // The access time should be sent here,
                // but it is not accessible with JavaAPI ;-<
                command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
                out.write(command.getBytes());
                out.flush();
            }
            //send "C0644 filesize filename", where filename should not include '/'
            long fileSize = _lfile.length();
            command = "C0644 " + fileSize + " ";
            if (source.lastIndexOf('/') > 0) {
                command += source.substring(source.lastIndexOf('/') + 1);
            } else {
                command += source;
            }
            command += "\n";
            out.write(command.getBytes());
            out.flush();

            //send content of file
            fileInputStream = new FileInputStream(source);
            byte[] buf = new byte[1024];
            long sum = 0;
            while (true) {
                int len = fileInputStream.read(buf, 0, buf.length);
                if (len <= 0) {
                    break;
                }
                out.write(buf, 0, len);
                sum += len;
            }
            //send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            return sum;
        } catch(JSchException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return -1;
    }



}
