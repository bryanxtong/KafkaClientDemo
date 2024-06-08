package org.example;

import java.util.Arrays;

/**
 * The Product json schema in files
 */
public class Product {
    private Integer productId;
    private String productName;
    private double price;
    private String[] tags;
    private Dimentions dimentions;

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    public Dimentions getDimentions() {
        return dimentions;
    }

    public void setDimentions(Dimentions dimentions) {
        this.dimentions = dimentions;
    }

    public static class Dimentions{
        private int length;
        private int width;
        private int height;

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public int getWidth() {
            return width;
        }

        public void setWidth(int width) {
            this.width = width;
        }

        public int getHeight() {
            return height;
        }

        public void setHeight(int height) {
            this.height = height;
        }
    }

    @Override
    public String toString() {
        return "Product{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                ", tags=" + Arrays.toString(tags) +
                ", dimentions=" + dimentions +
                '}';
    }
}
