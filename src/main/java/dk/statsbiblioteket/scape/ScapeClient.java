package dk.statsbiblioteket.scape;


import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import java.util.concurrent.TimeUnit;

public abstract class ScapeClient {


    private final Client client;
    private String service;
    private String username;
    private String password;

    protected ScapeClient(String service, String username, String password) {
        this.service = service;
        this.username = username;
        this.password = password;
        ClientConfig config = new DefaultClientConfig();
        client = Client.create(config);

    }

    protected AsyncWebResource request() {
        AsyncWebResource restApi = client.asyncResource(service);
        restApi.addFilter(new HTTPBasicAuthFilter(username,password.getBytes()));
        return restApi;
    }

    protected void destroy() {
        client.getExecutorService().shutdown();
        try {
            client.getExecutorService().awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        client.destroy();
    }

}
