package micro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NetClient<T> {
    public static final Logger logger = LoggerFactory.getLogger(NetClient.class);

    private final static long TIMEOUT = 20*1000;
    private Set<String> servers;
    private int currentServer;
    private String tag;
    private boolean wasError;
    private  Socket socket;
    private BufferedReader input;
    private OutputStream output;

    public NetClient(String tag) {
        this.tag=tag;
        this.wasError=false;
        this.currentServer=0;
        discoverServer(TIMEOUT);
    }

    public NetClient(String tag,long timeout) {
        this.tag=tag;
        this.wasError=false;
        this.currentServer=0;
        discoverServer(timeout);
    }

    public void disconnect() {
        try {
            if (socket!=null) {
                input.close();
                output.close();
                socket.close();
                socket.close();
                socket = null;
                logger.info("client disconnected");
            }
        }catch (IOException e) {
            logger.error(e.toString());
        }
    }

    public NetClient<T> connectNext() {
        if (currentServer>0) {
            disconnect();
            if (currentServer >= servers.size() - 1)
                currentServer = 0;
        }
        List<String>  list=servers.stream().collect(Collectors.toList());
        int ntry=0;
        while(ntry<5) {
                for (int i = currentServer; i < list.size(); i++) {
                    try {
                        String addr = list.get(i);
                        String[] hostPort = addr.split(":");
                        if (hostPort.length != 2) continue;
                        socket = new Socket(hostPort[0], Integer.parseInt(hostPort[1]));
                        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        output = socket.getOutputStream();
                        logger.info("client " + hostPort[0] + ":" + hostPort[1] + "(" + i + ") connected");
                        currentServer = i + 1;
                        return this;
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error(e.toString());
                    }
                }
            try {
                Thread.sleep(1000);
            }catch (InterruptedException e){}
            ntry++;
        }
        wasError=true;
        logger.error("there is no server");
        return this;
    }

    public T load(T o) {
        return (T)send(NetPoint.COMMANDS.LOAD,(T)o,false);
    }

    public T loadAndClose(T o) {
        return (T)send(NetPoint.COMMANDS.LOAD,(T)o,true);
    }

    public T save(T o) {
        return (T)send(NetPoint.COMMANDS.SAVE,(T)o,false);
    }

    public T delete(T o) {
        return (T)send(NetPoint.COMMANDS.DELETE,(T)o,false);
    }

    public Object send(NetPoint.COMMANDS cmd,Object o,boolean toClose) {
        try {
            if (wasError) return null;
            String json = new ObjectMapper().writeValueAsString(o);
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            bo.write((cmd.name()+",").getBytes());
            bo.write((o.getClass().getName()+"\n").getBytes());
            bo.write((json+"\n").getBytes());
            output.write(bo.toByteArray());
            logger.info("sent:"+o.getClass().getName()+","+json);
            String clsname=input.readLine();
            json=input.readLine();
            if (clsname==null  || json==null) {
               logger.error("send - "+clsname+","+json);
                return null;
            }
            try {
                Object reply=new ObjectMapper().readValue(json, Class.forName(clsname));
                logger.info("read:"+reply.getClass().getName());
                return reply;
            }catch(ClassNotFoundException e) {
            }
        }catch(IOException e){
            e.printStackTrace();

            logger.error(e.toString());
        } finally {
            if (toClose)
                disconnect();
        }
        return null;
    }

    private void discoverServer(long timeout) {
        try {
            MulticastSocket socket = new MulticastSocket(NetPoint.MULTICAST_PORT);
            InetAddress group = InetAddress.getByName(NetPoint.MULTICAST_ADDRESS);
            socket.joinGroup(group);
            long start=System.currentTimeMillis();
            while(System.currentTimeMillis()-start<=timeout) {
                byte[] buf = new byte[1024];
                DatagramPacket pckt = new DatagramPacket(buf, buf.length);
                socket.receive(pckt);
                byte[] d=pckt.getData();
                int len=pckt.getLength();
                String announce =  new String(d, 0, len);
                String[] tags=announce.split(",");
                if (tags==null || tags.length<3) continue;
                if (!tags[0].equals("mnc")
                        || !tags[1].equals(tag)) continue;
                logger.info("discovered server: "+tag);
                servers=new HashSet<>();
                for(int i=1;i< tags.length;i++) servers.add(tags[i]);
                try{
                Thread.sleep(100); } catch(InterruptedException e){}
            }
            socket.leaveGroup(group);
            socket.close();
        }catch(IOException e){
            logger.error(e.toString());
        }
    }

}
