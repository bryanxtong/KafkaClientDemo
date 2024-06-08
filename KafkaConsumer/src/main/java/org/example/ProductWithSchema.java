package org.example;

import java.util.Arrays;

/**
 * pass a schema directly to the producer using annotations on the Java class
 */
@io.confluent.kafka.schemaregistry.annotations.Schema(value = ProductWithSchema.SCHEMA_AS_STRING,
        refs = {})
public class ProductWithSchema {
    public static final String SCHEMA_AS_STRING = """
                {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                            "$id": "https://example.com/product.schema.json",
                            "title": "Product",
                            "description": "A product from Acme's catalog",
                            "type": "object",
                            "properties": {
                        "productId": {
                            "description": "The unique identifier for a product",
                                    "type": "integer"
                        },
                        "productName": {
                            "description": "Name of the product",
                                    "type": "string"
                        },
                        "price": {
                            "description": "The price of the product",
                                    "type": "number",
                                    "exclusiveMinimum": 0
                        },
                        "tags": {
                            "description": "Tags for the product",
                                    "type": "array",
                                    "items": {
                                "type": "string"
                            },
                            "minItems": 1,
                                    "uniqueItems": true
                        }
                    },
                    "required": [ "productId", "productName", "price" ]
                }
            """;

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

    public static class Dimentions {
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
        return "ProductWithSchema{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                ", tags=" + Arrays.toString(tags) +
                ", dimentions=" + dimentions +
                '}';
    }
}
