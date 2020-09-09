package micro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiFunction;


public class NetPoint {
    public static final Logger logger = LoggerFactory.getLogger(NetPoint.class);

    public enum COMMANDS {
        LOAD,SAVE,DELETE
    }

    public  final static int MULTICAST_PORT=4555;
    public final static String MULTICAST_ADDRESS="225.5.5.5";
    private final static long MULTICAST_INTERVAL= 5 *1000;
    private static NetPoint  net;
    private String tag;
    private int startedClients=0;
    private int maxClients;
    private volatile int currClients=0;
    private  BiFunction<COMMANDS,Object,Object> consumer;
    private ExecutorService executor;
    private ExecutorService workersExecutor;
    private List<InetAddress> inetAddresses;
    private List<Integer> inetPorts;
    private List<Server> servers = new ArrayList<>();
    private BlockingDeque<ServerWorker> clients = new LinkedBlockingDeque<>();

    private NetPoint(String tag, int maxClients,  BiFunction<COMMANDS,Object,Object> consumer) {
        this.tag=tag;
        this.maxClients=maxClients;
        this.consumer=consumer;
        inetAddresses=loadAllInterfaces();
        inetPorts=findAvailablePorts();

    }

    public static NetPoint start(String tag, int maxClients, BiFunction<COMMANDS,Object,Object> consumer ) {
        if (net==null) {
            net=new NetPoint(tag,maxClients,consumer);
            net.startServers();
        }
        return net;
    }

    public void stop() {
        stopServers();
        stopWorkers();
        net=null;
    }

    private List<InetAddress> loadAllInterfaces() {
        ArrayList<InetAddress> result = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> list = NetworkInterface.getNetworkInterfaces();
            while (list.hasMoreElements()) {
                NetworkInterface i = list.nextElement();
                if (!i.isUp()) continue;
                for (InterfaceAddress a : i.getInterfaceAddresses()) {
                    if (a.getAddress() instanceof Inet6Address) continue;
                    result.add(a.getAddress());
                    logger.info("interface: "+a.getAddress().getHostAddress());
                }
            }
        } catch (IOException  e){ }
        return result;
    }


    private List<Integer>  findAvailablePorts() {
        ArrayList<Integer> ports = new ArrayList<>();
        for(InetAddress a:inetAddresses) {
            try (ServerSocket socket = new ServerSocket(0,0,a)) {
                ports.add(socket.getLocalPort());
                continue;
            } catch (IOException e) {
            }
            ports.add(0);
        }
        return ports;
    }

    private void startServers() {
        for(int i=0;i<inetAddresses.size();i++) {
            Server srv=new Server(inetAddresses.get(i),inetPorts.get(0));
            servers.add(srv);
        }
        workersExecutor = Executors.newFixedThreadPool(maxClients*servers.size());
        executor = Executors.newFixedThreadPool(servers.size()+1);
        for(Server s:servers)
            executor.submit(s);
        executor.submit(new Broadcaster());
    }

    private void stopWorkers() {
        workersExecutor.shutdown();
        try {
            if (!workersExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                workersExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            workersExecutor.shutdownNow();
        }
    }

    private void stopServers() {
        executor.shutdown();
        try {
            for(Server s:servers) s.disconnect();
            if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }


    class ServerWorker implements  Runnable {
        Socket socket;
        BufferedReader input;
        OutputStream output;
        Semaphore semaphore;

        public ServerWorker() {
            semaphore=new Semaphore(1);
            try{
             semaphore.acquire();
            }catch (InterruptedException e){}
        }

        public boolean isFree() {
            return socket==null && semaphore.availablePermits()==0;
        }

        public void start(Socket socket) {
            this.socket=socket;
            semaphore.release();
        }

        public void stop() {
            disconnect();
            this.socket=null;
        }

        public void disconnect() {
            try {
                input.close();
                output.close();
                socket.close();
                logger.info("server disconnected");
            }catch (IOException e) {
                logger.error(e.toString());
            }
        }

        public void run() {
            while (true) {
                try {
                    while (!semaphore.tryAcquire(100, TimeUnit.MICROSECONDS)) ;
                    logger.info("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + " (" + currClients + ") started");
                    input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    output = socket.getOutputStream();
                    while (!workersExecutor.isShutdown()) {
                        String line = input.readLine();
                        if (line==null) break;
                        String[] arr=line.split(",");
                        String clsname=arr[1];
                        COMMANDS cmd= COMMANDS.valueOf(arr[0]);
                        String json = input.readLine();
                        if (json == null) break;
                        logger.info("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + " read:" + clsname + "," + json);
                        try {
                            Object o = new ObjectMapper().readValue(json, Class.forName(clsname));
                            Object reply = consumer.apply(cmd,o);
                            if (reply != null) {
                                json = new ObjectMapper().writeValueAsString(reply);
                                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                                bo.write((reply.getClass().getName()+"\n").getBytes());
                                bo.write((json+"\n").getBytes());
                                output.write(bo.toByteArray());
                                logger.info("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + " write:" + reply.getClass().getName() + "," + json);
                            }
                        } catch (ClassNotFoundException e) {
                            logger.error("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + " - " + e.toString());
                        }
                    }
                    logger.info("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + " stopped");
                }catch(IOException | InterruptedException e){
                    logger.error("worker " + socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort() + "-" + e.toString());
                } finally{
                    stop();
                }
            }
        }

    }


    class Server implements Runnable {
        ServerSocket socket;
        InetAddress address;
        Integer port;
        boolean started=false;

        public Server(InetAddress address, Integer port) {
            this.address = address;
            this.port = port;
        }
        public boolean isStarted() {
            return started;
        }
        public void disconnect()  {
            try {
                socket.close();
            }catch (IOException e) {
                logger.error(e.toString());
            }
        }
        void warmUpThreads() {
            for(int i=0;i<maxClients;i++) {
                ServerWorker c=new ServerWorker();
                clients.add(c);
                workersExecutor.submit(c);
            }
        }
        ServerWorker getFreeWorker() {
            long start=System.currentTimeMillis();
            ServerWorker worker = clients.poll();
            while (worker!=null
                    && !worker.isFree()
                    && System.currentTimeMillis()-start<1000) {
                worker = clients.poll();
            }
            return worker.isFree()?worker:null;
        }
        public void run() {
            while (!executor.isShutdown()) {
                try {
                    warmUpThreads();
                    socket = new ServerSocket(port, 0, address);
                    socket.setReuseAddress(true);
                    logger.info(address.getHostAddress()+":"+port+" started");
                    started=true;
                    while (!executor.isShutdown()) {
                        Socket clientSocket = socket.accept();
                        if (clientSocket != null) {
                            ServerWorker worker=getFreeWorker();
                            if (worker!=null) {
                                worker.start(clientSocket);
                                clients.addLast(worker);
                            } else {
                                clientSocket.close();
                                continue;
                            };
                        }
                    }
                    logger.info(address.getHostAddress()+":"+port+" stopped");
                } catch (IOException e) {
                    logger.error(address.getHostAddress()+":"+port+"-"+e.toString());
                }
            }
        }
    }

    class Broadcaster implements Runnable {
        MulticastSocket socket = null;
        String msg;
        public Broadcaster() {
            StringBuilder b=new StringBuilder();
            b.append("mnc,");
            b.append(tag);
            b.append(",");
            for(int i=0;i<inetAddresses.size();i++) {
                if (i>0) b.append(",");
                b.append(inetAddresses.get(i).getHostAddress()+":"+inetPorts.get(0));
            }
            msg=b.toString();
        }
        public void disconnect() {
            socket.close();
        }
        public void run() {
            while(!executor.isShutdown()) {
                try {
                    socket = new MulticastSocket(MULTICAST_PORT);
                    logger.info("multicast server "+MULTICAST_ADDRESS+":"+MULTICAST_PORT+" started");
                    byte[] buf = msg.getBytes();
                    InetAddress address = InetAddress.getByName(MULTICAST_ADDRESS);
                    DatagramPacket pckt = new DatagramPacket(buf, buf.length, address, MULTICAST_PORT);
                    while(!executor.isShutdown()) {
                        if (currClients<maxClients) {
                            socket.send(pckt);
                            logger.info("multicast server "+MULTICAST_ADDRESS+":"+MULTICAST_PORT+" a packet sent: ("+currClients+" of "+maxClients+")");
                        }
                        Thread.sleep(MULTICAST_INTERVAL);
                    }
                    socket.close();
                    logger.info("multicast server "+MULTICAST_ADDRESS+":"+MULTICAST_PORT+" stopped");
                }catch(IOException | InterruptedException e) {
                    logger.error("multicast server "+MULTICAST_ADDRESS+":"+MULTICAST_PORT+"-"+e.toString());
                }
            }
        }
    }


}
