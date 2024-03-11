package org.example;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.nio.file.Files;
import java.nio.file.Paths;
public class Main {
    static String uID;
    static {
        try {
            uID = getUID();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws MqttException, IOException {

        String brokerAddress = "tcp://139.150.83.249:1883";
        String username = "root";
        String password = "public";
        String clientId = "Pub";
        MqttClient client = new MqttClient(brokerAddress, clientId);
        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);
        conOpt.setUserName(username);
        conOpt.setPassword(password.toCharArray());

        client.connect(conOpt);
        System.out.println("Connected");
        //getUID();
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(10);
        exec.scheduleAtFixedRate(() -> {
            try {
                HeartBeat(client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        },0,5, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {getWthrWrnMsg(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_ocean(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_oceandata(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_UV(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_ATMO(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing_WAVE(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing2(client);},0,30, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(() -> {Publishing3(client);},0,30, TimeUnit.MINUTES);
        //client.disconnect();
    }
    //장비 uID
    public static String getUID() throws IOException {
        //serialNo -> topic serialNo
        URL url = new URL("https://pohang.ictpeople.co.kr/api/Equipment/GetEquipment?SerialNo=DX20240220-0001");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        String uID = null;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream inputStream = connection.getInputStream();
            byte[] responseData = inputStream.readAllBytes();
            String response = new String(responseData);
            JSONObject jsonObject = new JSONObject(response);
            JSONArray array = jsonObject.getJSONArray("data");
            JSONObject obj = (JSONObject) array.get(0);
            uID = obj.getString("serialNo");
            //System.out.println(uID);

        } else {
            System.out.println("HTTP GET 요청 실패: " + responseCode);
        }
        return uID;
    }
    //주기 데이터
    public static void HeartBeat(MqttClient client) throws Exception {
        String val = "heartbeat data";
        String jsonfile = readFileAsString("C:\\MQTTPub\\src\\sample.json");
        //String jsonfile = readFileAsString("/app/sample.json");
        String topic = "PohangPG/"+ uID +"/heartbeat";
        MqttMessage message = new MqttMessage(val.getBytes());
        MqttMessage message2 = new MqttMessage(jsonfile.getBytes());
        message.setQos(0);
        try {
            client.publish(topic, message);
            System.out.println("heartbeat publish");
            client.publish(topic, message2);
            System.out.println("sample.json publish");
            System.out.println(message2);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }
    //sample.json Read
    private static String readFileAsString(String s) throws Exception {
        return new String(Files.readAllBytes(Paths.get(s)));
    }
    //초단기실황 API
    public static void Publishing (MqttClient client){

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey=P5M6l%2BAiLQ3PknQ%2FC4KyDqPZx22%2FyLVYcX%2Feq%2FkuSWlrTbz2okiCsU3ih2pSsydn%2ForpSlFMP2XwsgTOmp3cYA%3D%3D&numOfRows=10&pageNo=1&base_date="+currentDay+"&base_time="+currentTime+"&nx=102&ny=94&&dataType=JSON";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for(int i = 0; i<items.length();i++){
                JSONObject item = items.getJSONObject(i);
                String category = item.getString("category");
                //category + Value publish
                switch (category) {
                    //obsrvalue : 코드값 0 : 없음, 1 : 비, 2 : 비/눈, 3 : 눈 , 5 : 빗방울, 6 : 빗방울날림, 7 : 눈날림
                    case "PTY":
                        //String val = String.join(", ", "rain : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val = item.toString();
                        String topic = "PohangPG/"+uID+"/rainNcst";
                        MqttMessage message = new MqttMessage(val.getBytes());
                        message.setQos(0);
                        try {
                            client.publish(topic, message);
                            System.out.println("강수형태:" + message);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "REH":
                        //String val2 = "hum : " + obsrValue;
                        //String val2 = String.format("hum : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val2 = String.join(", ", "hum : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val2 = item.toString();
                        String topic2 = "PohangPG/"+ uID +"/humNcst";
                        MqttMessage message2 = new MqttMessage(val2.getBytes());
                        message2.setQos(0);
                        try {
                            client.publish(topic2, message2);
                            System.out.println("습도:" + message2);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "RN1":
                        //String val3 = "hourRain : " + obsrValue + "mm";
                        //String val3 = String.format("hourRain : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val3 = String.join(", ", "hourRain : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val3 = item.toString();
                        String topic3 = "PohangPG/"+ uID +"/hourRainNcst";
                        MqttMessage message3 = new MqttMessage(val3.getBytes());
                        message3.setQos(0);
                        try {
                            client.publish(topic3, message3);
                            System.out.println("1시간 강수량:" + message3);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "T1H":
                        //String val4 = "temp : " + obsrValue + "C";
                        //String val4 = String.format("temp : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val4 = String.join(", ", "temp : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val4 = item.toString();
                        String topic4 = "PohangPG/"+ uID +"/tempNcst";
                        MqttMessage message4 = new MqttMessage(val4.getBytes());
                        message4.setQos(0);
                        try {
                            client.publish(topic4, message4);
                            System.out.println("기온:" + message4);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "UUU":
                        //String val5 = "wind : " + obsrValue + "m/s";
                        //String val5 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val5 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val5 = item.toString();
                        String topic5 = "PohangPG/"+ uID +"/weWindNcst";
                        MqttMessage message5 = new MqttMessage(val5.getBytes());
                        message5.setQos(0);
                        try {
                            client.publish(topic5, message5);
                            System.out.println("동서바람성분:" + message5);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VEC":
                        //String val6 = "windDir : " + obsrValue + "deg";
                        //String val6 = String.format("windDir : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val6 = String.join(", ", "windDir : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val6 = item.toString();
                        String topic6 = "PohangPG/"+ uID +"/windDirNcst";
                        MqttMessage message6 = new MqttMessage(val6.getBytes());
                        message6.setQos(0);
                        try {
                            client.publish(topic6, message6);
                            System.out.println("풍향:" + message6);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VVV":
                        //String val7 = "wind : " + obsrValue + "m/s";
                        //String val7 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val7 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val7 = item.toString();
                        String topic7 = "PohangPG/"+ uID +"/snWindNcst";
                        MqttMessage message7 = new MqttMessage(val7.getBytes());
                        message7.setQos(0);
                        try {
                            client.publish(topic7, message7);
                            System.out.println("남북바람성분:" + message7);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WSD":
                        //String val8 = "wind : " + obsrValue + "m/s";
                        //String val8 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val8 = String.join(", ", "wind : " + obsrValue, "basedate : " + date, "basetime : " + time, "nx : " + nx, "ny : " + ny);
                        String val8 = item.toString();
                        String topic8 = "PohangPG/"+ uID +"/windSpeedNcst";
                        MqttMessage message8 = new MqttMessage(val8.getBytes());
                        message8.setQos(0);
                        try {
                            client.publish(topic8, message8);
                            System.out.println("풍속:" + message8);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                }
            }

        });
        responseFuture.join();
    }
    //초단기예보 API
    public static void Publishing2 (MqttClient client){

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst?serviceKey=P5M6l%2BAiLQ3PknQ%2FC4KyDqPZx22%2FyLVYcX%2Feq%2FkuSWlrTbz2okiCsU3ih2pSsydn%2ForpSlFMP2XwsgTOmp3cYA%3D%3D&numOfRows=60&pageNo=1&base_date="+currentDay+"&base_time="+currentTime+"&nx=102&ny=94&&dataType=JSON";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for(int i = 5; i<items.length();i+=6){
                JSONObject item = items.getJSONObject(i);
                String category = item.getString("category");
                String fcstValue = item.getString("fcstValue");
                String fcstTime = item.getString("fcstTime");
                String fcstDate = item.getString("fcstDate");
                int nx = item.getInt("nx");
                int ny = item.getInt("ny");
                //category + Value publish
                switch (category) {
                    //obsrvalue : 코드값 0 : 없음, 1 : 비, 2 : 비/눈, 3 : 눈 , 5 : 빗방울, 6 : 빗방울날림, 7 : 눈날림
                    case "PTY":
                        //String val = String.join(", ", "rain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val = item.toString();
                        String topic = "PohangPG/"+uID+"/rainFcst";
                        MqttMessage message = new MqttMessage(val.getBytes());
                        message.setQos(0);
                        try {
                            client.publish(topic, message);
                            System.out.println("강수형태:" + message);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "REH":
                        //String val2 = "hum : " + obsrValue;
                        //String val2 = String.format("hum : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val2 = String.join(", ", "hum : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val2 = item.toString();
                        String topic2 = "PohangPG/"+ uID +"/humFcst";
                        MqttMessage message2 = new MqttMessage(val2.getBytes());
                        message2.setQos(0);
                        try {
                            client.publish(topic2, message2);
                            System.out.println("습도:" + message2);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "RN1":
                        //String val3 = "hourRain : " + obsrValue + "mm";
                        //String val3 = String.format("hourRain : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val3 = String.join(", ", "hourRain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val3 = item.toString();
                        String topic3 = "PohangPG/"+ uID +"/hourRainFcst";
                        MqttMessage message3 = new MqttMessage(val3.getBytes());
                        message3.setQos(0);
                        try {
                            client.publish(topic3, message3);
                            System.out.println("1시간 강수량:" + message3);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "T1H":
                        //String val4 = "temp : " + obsrValue + "C";
                        //String val4 = String.format("temp : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val4 = String.join(", ", "temp : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val4 = item.toString();
                        String topic4 = "PohangPG/"+ uID +"/tempFcst";
                        MqttMessage message4 = new MqttMessage(val4.getBytes());
                        message4.setQos(0);
                        try {
                            client.publish(topic4, message4);
                            System.out.println("기온:" + message4);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "UUU":
                        //String val5 = "wind : " + obsrValue + "m/s";
                        //String val5 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val5 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val5 = item.toString();
                        String topic5 = "PohangPG/"+ uID +"/weWindFcst";
                        MqttMessage message5 = new MqttMessage(val5.getBytes());
                        message5.setQos(0);
                        try {
                            client.publish(topic5, message5);
                            System.out.println("동서바람성분:" + message5);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VEC":
                        //String val6 = "windDir : " + obsrValue + "deg";
                        //String val6 = String.format("windDir : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val6 = String.join(", ", "windDir : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val6 = item.toString();
                        String topic6 = "PohangPG/"+ uID +"/windDirFcst";
                        MqttMessage message6 = new MqttMessage(val6.getBytes());
                        message6.setQos(0);
                        try {
                            client.publish(topic6, message6);
                            System.out.println("풍향:" + message6);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VVV":
                        //String val7 = "wind : " + obsrValue + "m/s";
                        //String val7 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val7 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstTime : " + fcstDate);
                        String val7 = item.toString();
                        String topic7 = "PohangPG/"+ uID +"/snWindFcst";
                        MqttMessage message7 = new MqttMessage(val7.getBytes());
                        message7.setQos(0);
                        try {
                            client.publish(topic7, message7);
                            System.out.println("남북바람성분:" + message7);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WSD":
                        //String val8 = "wind : " + obsrValue + "m/s";
                        //String val8 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val8 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val8 = item.toString();
                        String topic8 = "PohangPG/"+ uID +"/windSpeedFcst";
                        MqttMessage message8 = new MqttMessage(val8.getBytes());
                        message8.setQos(0);
                        try {
                            client.publish(topic8, message8);
                            System.out.println("풍속:" + message8);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 코드값 1 : 맑음, 3 : 구름많음, 4 : 흐림
                    case "SKY":
                        //String val9 = String.join(", ", "SKY : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val9 = item.toString();
                        String topic9 = "PohangPG/"+ uID +"/skyFcst";
                        MqttMessage message9 = new MqttMessage(val9.getBytes());
                        message9.setQos(0);
                        try {
                            client.publish(topic9, message9);
                            System.out.println("하늘:" + message9);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 낙뢰(초단기예보) : 에너지밀도(0.2~100KA(킬로암페어)/㎢)
                    case "LGT":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val10 = item.toString();
                        String topic10 = "PohangPG/"+ uID +"/lgtFcst";
                        MqttMessage message10 = new MqttMessage(val10.getBytes());
                        message10.setQos(0);
                        try {
                            client.publish(topic10, message10);
                            System.out.println("낙뢰:" + message10);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                }
            }

        });
        responseFuture.join();
    }
    //단기 예보 API
    public static void Publishing3 (MqttClient client){

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst?serviceKey=P5M6l%2BAiLQ3PknQ%2FC4KyDqPZx22%2FyLVYcX%2Feq%2FkuSWlrTbz2okiCsU3ih2pSsydn%2ForpSlFMP2XwsgTOmp3cYA%3D%3D&numOfRows=10&pageNo=1&base_date="+currentDay+"&base_time="+currentTime+"&nx=102&ny=94&dataType=JSON";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for(int i = 0; i<items.length();i++){
                JSONObject item = items.getJSONObject(i);
                String category = item.getString("category");
                //category + Value publish
                switch (category) {
                    case "POP":
                        //String val = String.join(", ", "rain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val = item.toString();
                        String topic = "PohangPG/"+uID+"/POPVF";
                        MqttMessage message = new MqttMessage(val.getBytes());
                        message.setQos(0);
                        try {
                            client.publish(topic, message);
                            System.out.println(topic);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "PTY":
                        //String val2 = "hum : " + obsrValue;
                        //String val2 = String.format("hum : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val2 = String.join(", ", "hum : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val2 = item.toString();
                        String topic2 = "PohangPG/"+ uID +"/PTYVF";
                        MqttMessage message2 = new MqttMessage(val2.getBytes());
                        message2.setQos(0);
                        try {
                            client.publish(topic2, message2);
                            System.out.println(topic2);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "PCP":
                        //String val3 = "hourRain : " + obsrValue + "mm";
                        //String val3 = String.format("hourRain : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val3 = String.join(", ", "hourRain : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val3 = item.toString();
                        String topic3 = "PohangPG/"+ uID +"/PCPVF";
                        MqttMessage message3 = new MqttMessage(val3.getBytes());
                        message3.setQos(0);
                        try {
                            client.publish(topic3, message3);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "REH":
                        //String val4 = "temp : " + obsrValue + "C";
                        //String val4 = String.format("temp : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val4 = String.join(", ", "temp : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val4 = item.toString();
                        String topic4 = "PohangPG/"+ uID +"/REHVF";
                        MqttMessage message4 = new MqttMessage(val4.getBytes());
                        message4.setQos(0);
                        try {
                            client.publish(topic4, message4);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "SNO":
                        //String val5 = "wind : " + obsrValue + "m/s";
                        //String val5 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val5 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val5 = item.toString();
                        String topic5 = "PohangPG/"+ uID +"/SNOVF";
                        MqttMessage message5 = new MqttMessage(val5.getBytes());
                        message5.setQos(0);
                        try {
                            client.publish(topic5, message5);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "SKY":
                        //String val6 = "windDir : " + obsrValue + "deg";
                        //String val6 = String.format("windDir : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val6 = String.join(", ", "windDir : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val6 = item.toString();
                        String topic6 = "PohangPG/"+ uID +"/SKYVF";
                        MqttMessage message6 = new MqttMessage(val6.getBytes());
                        message6.setQos(0);
                        try {
                            client.publish(topic6, message6);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "TMP":
                        //String val7 = "wind : " + obsrValue + "m/s";
                        //String val7 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val7 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstTime : " + fcstDate);
                        String val7 = item.toString();
                        String topic7 = "PohangPG/"+ uID +"/TMPVF";
                        MqttMessage message7 = new MqttMessage(val7.getBytes());
                        message7.setQos(0);
                        try {
                            client.publish(topic7, message7);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "TMN":
                        //String val8 = "wind : " + obsrValue + "m/s";
                        //String val8 = String.format("wind : %s, basedate : %s, basetime : %s, nx : %d, ny : %d" + obsrValue,date,time,nx,ny);
                        //String val8 = String.join(", ", "wind : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val8 = item.toString();
                        String topic8 = "PohangPG/"+ uID +"/TMNVF";
                        MqttMessage message8 = new MqttMessage(val8.getBytes());
                        message8.setQos(0);
                        try {
                            client.publish(topic8, message8);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 코드값 1 : 맑음, 3 : 구름많음, 4 : 흐림
                    case "TMX":
                        //String val9 = String.join(", ", "SKY : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val9 = item.toString();
                        String topic9 = "PohangPG/"+ uID +"/TMXVF";
                        MqttMessage message9 = new MqttMessage(val9.getBytes());
                        message9.setQos(0);
                        try {
                            client.publish(topic9, message9);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    // fcstvalue : 낙뢰(초단기예보) : 에너지밀도(0.2~100KA(킬로암페어)/㎢)
                    case "UUU":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val10 = item.toString();
                        String topic10 = "PohangPG/"+ uID +"/UUUVF";
                        MqttMessage message10 = new MqttMessage(val10.getBytes());
                        message10.setQos(0);
                        try {
                            client.publish(topic10, message10);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VVV":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val11 = item.toString();
                        String topic11 = "PohangPG/"+ uID +"/VVVVF";
                        MqttMessage message11 = new MqttMessage(val11.getBytes());
                        message11.setQos(0);
                        try {
                            client.publish(topic11, message11);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WAV":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val12 = item.toString();
                        String topic12 = "PohangPG/"+ uID +"/WAVVF";
                        MqttMessage message12 = new MqttMessage(val12.getBytes());
                        message12.setQos(0);
                        try {
                            client.publish(topic12, message12);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "VEC":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val13 = item.toString();
                        String topic13 = "PohangPG/"+ uID +"/VECVF";
                        MqttMessage message13 = new MqttMessage(val13.getBytes());
                        message13.setQos(0);
                        try {
                            client.publish(topic13, message13);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "WSD":
                        //String val10 = String.join(", ", "LGT : " + fcstValue, "nx : " + nx, "ny : " + ny,"fcstTime : " + fcstTime,"fcstDate : " + fcstDate);
                        String val14 = item.toString();
                        String topic14 = "PohangPG/"+ uID +"/WSDVF";
                        MqttMessage message14 = new MqttMessage(val14.getBytes());
                        message14.setQos(0);
                        try {
                            client.publish(topic14, message14);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                }
            }

        });
        responseFuture.join();
    }
    //최신 해양 데이터 API tideObsRecent
    public static void Publishing_ocean(MqttClient client) {
        String url = "https://www.khoa.go.kr/api/oceangrid/tideObsRecent/search.do?ServiceKey=EDrcCIFdi7qEsdqj0mgrjQ==&ObsCode=DT_0091&ResultType=json";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject obj = new JSONObject(responseBody);
            JSONObject data = new JSONObject();
            data.put("meta",obj.getJSONObject("result").getJSONObject("meta"));
            data.put("data",obj.getJSONObject("result").getJSONObject("data"));

            String topic = "PohangPG/" + uID + "/ocean";
            MqttMessage message = new MqttMessage(data.toString().getBytes());
            message.setQos(0);
            try{
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException me){
                throw new RuntimeException(me);
            }

        });
        responseFuture.join();
    }
    //[tideCurPre,tideObsTemp,tideObsSalt,tideObsAirTemp,tideObsAirPres,tideObsWind]
    public static void Publishing_oceandata(MqttClient client){
        String[] arr = {"tideCurPre","tideObsTemp","tideObsSalt","tideObsAirTemp","tideObsAirPres","tideObsWind"};
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);
        for(int i = 0; i<arr.length;i++){
            String topic = "PohangPG/" + uID + "/" + arr[i];
            String url = "https://www.khoa.go.kr/api/oceangrid/"+arr[i]+"/search.do?ServiceKey=JP3zOlsaukF2ueEpQUJj7g==&ObsCode=DT_0091&Date="+currentDay+"&ResultType=json";
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
            CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
            responseFuture.thenAccept(response -> {
                String responseBody = response.body();
                JSONObject obj = new JSONObject(responseBody);
                JSONObject data = new JSONObject();
                data.put("meta",obj.getJSONObject("result").getJSONObject("meta"));
                JSONArray dataArr = obj.getJSONObject("result").getJSONArray("data");
                for(int j = 0; j<dataArr.length();j++){
                    data.put("data",dataArr.getJSONObject(j));
                    System.out.println(data);
                    MqttMessage message = new MqttMessage(data.toString().getBytes());
                    message.setQos(0);
                    try{
                        client.publish(topic,message);
                        System.out.println(message);
                    } catch (MqttException me){
                        throw new RuntimeException(me);
                    }
                    data.remove("data");
                }
            });
            responseFuture.join();
        }

    }
    //자외선 API
    public static void Publishing_UV(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);
        String hour = currentTime.substring(0,2);

        String dayTime = currentDay + hour;
        String url = "https://apis.data.go.kr/1360000/LivingWthrIdxServiceV4/getUVIdxV4?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&pageNo=1&numOfRows=10&dataType=JSON&areaNo=4711155000&time=" + dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            JSONObject item = items.getJSONObject(0);
            String val = item.toString();
            String topic = "PohangPG/" + uID + "/uv";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }

    //대기 API
    public static void Publishing_ATMO(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);
        String hour = currentTime.substring(0,2);

        String dayTime = currentDay + hour;
        String url = "https://apis.data.go.kr/1360000/LivingWthrIdxServiceV4/getAirDiffusionIdxV4?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&pageNo=10&numOfRows=10&dataType=JSON&areaNo=4711155000&time="+dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            JSONObject item = items.getJSONObject(0);
            String val = item.toString();
            String topic = "PohangPG/" + uID + "/atmo";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }
    //파고 API
    public static void Publishing_WAVE(MqttClient client){
        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        //현재 시간 HHmm
        LocalTime Time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");
        String currentTime = Time.format(timeFormat);

        String dayTime = currentDay + currentTime;
        String url = "https://apis.data.go.kr/1360000/BeachInfoservice/getWhBuoyBeach?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&numOfRows=1&pageNo=10&dataType=JSON&beach_num=211&searchTime=" + dayTime;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            JSONObject item = items.getJSONObject(0);
            String val = item.toString();
            String topic = "PohangPG/" + uID + "/wave";
            MqttMessage message = new MqttMessage(val.getBytes());
            message.setQos(0);
            try {
                client.publish(topic,message);
                System.out.println(message);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }

        });
        responseFuture.join();
    }
    //기상 특보 API
    public static void getWthrWrnMsg(MqttClient client) {

        //현재 날짜 yyyyMMdd
        LocalDate Day = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
        String currentDay = Day.format(dateFormat);

        String url = "https://apis.data.go.kr/1360000/WthrWrnInfoService/getWthrWrnMsg?serviceKey=ClSg8YETAqDKd35FklryZ%2BYPkYWY6Q24PTpGdagqszHjRSWzowl08EhLqpdZKJvBkIUv%2FmrEcPwIK3iV8I3QVQ%3D%3D&pageNo=1&numOfRows=100&dataType=json&stnId=143&fromTmFc="+currentDay+"&toTmFc=" + currentDay;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(url)).build();
        //Data parsing
        CompletableFuture<HttpResponse<String>> responseFuture = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
        responseFuture.thenAccept(response -> {
            String responseBody = response.body();
            JSONObject Jsonobj = new JSONObject(responseBody);
            JSONObject bodyobj = Jsonobj.getJSONObject("response").getJSONObject("body");
            JSONArray items = bodyobj.getJSONObject("items").getJSONArray("item");
            for (int i = 0; i < items.length(); i++) {
                JSONObject item = items.getJSONObject(i);
                String val = item.toString();
                String topic = "PohangPG/" + uID + "/WthrWrnMsg";
                MqttMessage message = new MqttMessage(val.getBytes());
                message.setQos(0);
                try {
                    client.publish(topic, message);
                } catch (MqttException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }


}