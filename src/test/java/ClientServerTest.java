import micro.NetClient;
import micro.NetPoint;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

class TestObject {
    int id;
    String name;

    public TestObject() {

    }

    public TestObject(int id,String name) {
        this.id=id;
        this.name=name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

@RunWith(BlockJUnit4ClassRunner.class)
public class ClientServerTest {

    public ClientServerTest() {}

    @Test
    public void simpleSeqConnectionTest() {
        String name="Joe";
        String tag="A";

        NetPoint np=NetPoint.start(tag, 1,(cmd,o)->{
            if (o instanceof  TestObject) {
                if (((TestObject)o).id==100 && cmd==NetPoint.COMMANDS.LOAD) {
                    ((TestObject) o).name = name;
                    return o;
                }
                return null;
            }
            return null;
        });

        NetClient<TestObject> client=new NetClient<TestObject>(tag,0);
        for(int i=0;i<100;i++) {
            TestObject reply = client.connectNext().loadAndClose(new TestObject(100, "?"));
            assertNotNull(reply);
            assertTrue(reply instanceof TestObject);
            assertEquals(((TestObject) reply).name, name);
        }
        np.stop();
        try {
            Thread.sleep(1000);
        }catch(InterruptedException e){}
    }

    List<Future> results=new ArrayList<>();


    @Test
    public void parallelConnectionTest() {
        String name="Joe";
        String tag="A";
        NetPoint np=NetPoint.start(tag, 50,(cmd,o)->{
            if (o instanceof  TestObject) {
                if (((TestObject)o).id==100 && cmd==NetPoint.COMMANDS.LOAD) {
                    ((TestObject) o).name = name;
                    return o;
                }
                return null;
            }
            return null;
        });
        results.clear();

        Callable<Object> c = () -> {
            TestObject r= new NetClient<TestObject>(tag, 0)
                    .connectNext()
                    .loadAndClose(new TestObject(100, ""));
            return r;
        };
        ExecutorService service = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 50; i++) {
            results.add(service.submit(c));
        }
        try {
            while (!service.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                if (results.stream().filter(e->e.isDone()).count() == results.size())
                    break;
            }
            for (Future f:results) {
                Object reply = f.get();
                assertNotNull(reply);
                assertTrue(reply instanceof TestObject);
                assertEquals(((TestObject) reply).name, name);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        np.stop();
    }

    @Test
    public void sequentialQueriesTest() {
        String name="Joe";
        String tag="A";
        NetPoint np=NetPoint.start(tag, 1,(cmd,o)->{
            if (o instanceof  TestObject) {
                if (((TestObject)o).id==100 && cmd==NetPoint.COMMANDS.LOAD) {
                    ((TestObject) o).name = name;
                    return o;
                }
                return null;
            }
            return null;
        });

        NetClient client=new NetClient(tag,0).connectNext();
        for(int i=0;i<100;i++) {
            Object reply = client.load(new TestObject(100, "?"));
            assertNotNull(reply);
            assertTrue(reply instanceof TestObject);
            assertEquals(((TestObject) reply).name, name);
        }
        np.stop();
        try {
            Thread.sleep(1000);
        }catch(InterruptedException e){}
    }

}
