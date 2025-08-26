package com.crypto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 프리미엄 계산 결과를 나타내는 클래스
 * Redis에 저장될 데이터 구조
 */
public class PremiumResult {
    
    @JsonProperty("symbol")
    private String symbol;
    
    @JsonProperty("upbit_price")
    private double upbitPrice;
    
    @JsonProperty("binance_price")
    private double binancePrice;
    
    @JsonProperty("premium_rate")
    private double premiumRate;
    
    @JsonProperty("premium_amount")
    private double premiumAmount;
    
    @JsonProperty("upbit_volume")
    private double upbitVolume;
    
    @JsonProperty("binance_volume")
    private double binanceVolume;
    
    @JsonProperty("timestamp")
    private String timestamp;

    // 기본 생성자
    public PremiumResult() {}

    // 전체 생성자
    public PremiumResult(String symbol, double upbitPrice, double binancePrice, 
                        double premiumRate, double premiumAmount, 
                        double upbitVolume, double binanceVolume, String timestamp) {
        this.symbol = symbol;
        this.upbitPrice = upbitPrice;
        this.binancePrice = binancePrice;
        this.premiumRate = premiumRate;
        this.premiumAmount = premiumAmount;
        this.upbitVolume = upbitVolume;
        this.binanceVolume = binanceVolume;
        this.timestamp = timestamp;
    }

    /**
     * 두 PriceData에서 프리미엄 계산하여 결과 생성
     */
    public static PremiumResult calculate(PriceData upbit, PriceData binance, String timestamp) {
        double upbitKrw = upbit.getKrwPrice();
        double binanceKrw = binance.getKrwPrice();
        
        // 프리미엄 = (업비트가격 - 바이낸스가격) / 바이낸스가격 * 100
        double premiumAmount = upbitKrw - binanceKrw;
        double premiumRate = (premiumAmount / binanceKrw) * 100.0;
        
        return new PremiumResult(
            upbit.getNormalizedSymbol(),
            upbitKrw,
            binanceKrw,
            Math.round(premiumRate * 100.0) / 100.0, // 소수점 2자리 반올림
            Math.round(premiumAmount * 100.0) / 100.0,
            upbit.getVolume24h(),
            binance.getVolume24h(),
            timestamp
        );
    }

    // Getter and Setter methods
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    
    public double getUpbitPrice() { return upbitPrice; }
    public void setUpbitPrice(double upbitPrice) { this.upbitPrice = upbitPrice; }
    
    public double getBinancePrice() { return binancePrice; }
    public void setBinancePrice(double binancePrice) { this.binancePrice = binancePrice; }
    
    public double getPremiumRate() { return premiumRate; }
    public void setPremiumRate(double premiumRate) { this.premiumRate = premiumRate; }
    
    public double getPremiumAmount() { return premiumAmount; }
    public void setPremiumAmount(double premiumAmount) { this.premiumAmount = premiumAmount; }
    
    public double getUpbitVolume() { return upbitVolume; }
    public void setUpbitVolume(double upbitVolume) { this.upbitVolume = upbitVolume; }
    
    public double getBinanceVolume() { return binanceVolume; }
    public void setBinanceVolume(double binanceVolume) { this.binanceVolume = binanceVolume; }
    
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("PremiumResult{symbol='%s', premium=%.2f%%, upbit=%.0f, binance=%.0f}", 
                           symbol, premiumRate, upbitPrice, binancePrice);
    }
}