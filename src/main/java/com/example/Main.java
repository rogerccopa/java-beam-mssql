package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class Main {
    public static void main(String[] args) {
        // defined in my ~/.bashrc file
        final String DB_USERNAME = System.getenv("DB_USERNAME");
        final String DB_PASSWORD = System.getenv("DB_PASSWORD");

        final String DB_URL = 
            "jdbc:sqlserver://localhost:1433;" +
            "DatabaseName=test;" +
            "encrypt=true;" +
            "trustServerCertificate=true;";
        
        Connection conn = null;
        Statement stmt = null;

        Pipeline pipeline = Pipeline.create();
        
        try {
            conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
            stmt = conn.createStatement();
            String query = "SELECT id, title, quantity FROM test.dbo.products";
            ResultSet resultSet = stmt.executeQuery(query);

            List<String> records = new ArrayList<String>();
            int id;
            int qty;
            String title;

            while (resultSet.next()) {
                id = resultSet.getInt(1);
                title = resultSet.getString(2);
                qty = resultSet.getInt(3);
                
                records.add(String.valueOf(id) + "|" + title + "|" + String.valueOf(qty));
            }

            PCollection<String> items = pipeline.apply(
                "Transfor to uppercase",
                Create.of(records)
            );

            items.apply(
                TextIO.write().to("samplefile.txt").withoutSharding()
            );

            pipeline.run();

        } catch (Exception e) {
            System.out.println("Exception occurred" + e.getMessage());
        }
    }
}