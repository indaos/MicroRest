# MicroRest
<p>
A simple and dumb networking library that allows you to connect microservices to a local network without hard binding by ip and port.
</p>
<p>
Just start the server and client and they will find each other.You can send and receive objects and simple commands.
Everything is simple, but sometimes very easy and useful.It uses multicast to find the service and json to pass objects.
This can be useful if there are dozens of simple microservices that can be constantly migrating over the network and hundreds 
of clients are constantly making requests.These clients can make requests using round robin or first available service.
This helps to easily control and loosen the connection of the components.
</p>

<pre>
// no ip and port only type of service

String tag="LOCATION"

// Service
NetPoint np=NetPoint.start(tag, 100,(cmd,o)->{
        if (o instanceof  UserLocation) {
            if (cmd==NetPoint.COMMANDS.LOAD) {
                ((UserLocation) o).coordinates = "0,0";
                return o;
            }
            return null;
        }
        return null;
    });


//Client
 UserLocation result= new NetClient<UserLocation>(tag)
            .connectNext()
            .loadAndClose(new UserLocation("jan"));

</pre> 
