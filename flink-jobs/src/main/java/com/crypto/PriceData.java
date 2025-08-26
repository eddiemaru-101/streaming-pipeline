package com.crypto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 암호화폐 가격 데이터를 나타내는 POJO 클래스
 * Kafka에서 받은 JSON 메시지를 매핑
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PriceData {
    
    @JsonProperty("exchange")
    private String exchange;
    
    @JsonProperty("symbol")
    private String symbol;
    
    @JsonProperty("price")
    private double price;
    
    @JsonProperty("price_krw")  // 바이낸스에만 있음
    private Double priceKrw;
    
    @JsonProperty("volume_24h")
    private double volume24h;
    
    @JsonProperty("change_rate")
    private double changeRate;
    
    @JsonProperty("timestamp")
    private String timestamp;
    
    @JsonProperty("high_price")
    private double highPrice;
    
    @JsonProperty("low_price")
    private double lowPrice;

    // 기본 생성자 (Jackson 필요)
    public PriceData() {}

    // 전체 생성자
    public PriceData(String exchange, String symbol, double price, Double priceKrw, 
                    double volume24h, double changeRate, String timestamp, 
                    double highPrice, double lowPrice) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.price = price;
        this.priceKrw = priceKrw;
        this.volume24h = volume24h;
        this.changeRate = changeRate;
        this.timestamp = timestamp;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
    }

    // Getter and Setter methods
    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
    
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    
    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
    
    public Double getPriceKrw() { return priceKrw; }
    public void setPriceKrw(Double priceKrw) { this.priceKrw = priceKrw; }
    
    public double getVolume24h() { return volume24h; }
    public void setVolume24h(double volume24h) { this.volume24h = volume24h; }
    
    public double getChangeRate() { return changeRate; }
    public void setChangeRate(double changeRate) { this.changeRate = changeRate; }
    
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    
    public double getHighPrice() { return highPrice; }
    public void setHighPrice(double highPrice) { this.highPrice = highPrice; }
    
    public double getLowPrice() { return lowPrice; }
    public void setLowPrice(double lowPrice) { this.lowPrice = lowPrice; }

    /**
     * 업비트와 바이낸스 심볼을 통일된 형태로 변환
     * 예: KRW-BTC -> BTC, BTCUSDT -> BTC
     */
    public String getNormalizedSymbol() {
        if (symbol == null) return null;
        
        if (symbol.startsWith("KRW-")) {
            return symbol.substring(4); // "KRW-BTC" -> "BTC"
        } else if (symbol.endsWith("USDT")) {
            return symbol.substring(0, symbol.length() - 4); // "BTCUSDT" -> "BTC"
        }
        return symbol;
    }

    /**
     * KRW 가격 반환 (업비트는 price, 바이낸스는 price_krw)
     */
    public double getKrwPrice() {
        if ("upbit".equals(exchange)) {
            return price;
        } else if ("binance".equals(exchange) && priceKrw != null) {
            return priceKrw;
        }
        return 0.0;
    }

    @Override
    public String toString() {
        return String.format("PriceData{exchange='%s', symbol='%s', price=%.2f, priceKrw=%.2f, timestamp='%s'}", 
                           exchange, symbol, price, getKrwPrice(), timestamp);
    }
}