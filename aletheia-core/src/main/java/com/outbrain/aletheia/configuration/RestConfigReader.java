package com.outbrain.aletheia.configuration;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class RestConfigReader implements ConfigReader {
    @Override
    public InputStream read(URI configUri) throws IOException {
        URL url = new URL(configUri.toString());
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        InputStream stream = con.getInputStream();

        return stream;

    }


    public static void main(String[] args) throws URISyntaxException, IOException {
        RestConfigReader restConfigReader = new RestConfigReader();

        URI uri = new URI("http://gard-10101-prod-nydc1.nydc1.outbrain.com:58081/Gardien/version");
        InputStream is = restConfigReader.read(uri);


        System.out.println(IOUtils.toString(is));
    }
}
