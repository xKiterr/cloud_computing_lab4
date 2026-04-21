package cn.operations;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

import java.io.FileInputStream;
import java.io.IOException;

import static cn.operations.LabOperations.insertDocuments;

public class FirestoreSetup {

    public static Firestore initializeFirestore() throws IOException {
        String pathToServiceAccount = "/Users/kiter/Downloads/cn2526-t2-g10-503c9d50cfec.json";
        FileInputStream serviceAccount = new FileInputStream(pathToServiceAccount);

        FirestoreOptions firestoreOptions =
                FirestoreOptions.newBuilder()
                        .setProjectId("cn2526-t2-g10")
                        .setDatabaseId("cn2526-t2-g10-db")
                        .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                        .build();

        return firestoreOptions.getService();
    }

    public static void main(String[] args) {
        try {
            Firestore db = initializeFirestore();
            System.out.println("Successfully connected to database: cn2526-t2-g10-db");

            String colName = "ocupacoes";
            System.out.println("Starting Task 2 Tests:");
            //insertDocuments("OcupacaoEspacosPublicos.csv", db, "ocupacoes"); //comment it out after first run
            
            //2.a: get specific document
            LabOperations.printDocumentById(db, colName, "Lab4-2017");

            //2.b: delete a specific field (e.g., delete 'local' from doc Lab4-2017)
            LabOperations.deleteFieldFromDocument(db, colName, "Lab4-2017", "location.local");

            //2.c: simple query (parish)
            LabOperations.getDocumentsByParish(db, colName, "Belém");

            //2.d: compound query
            LabOperations.getCompoundQuery(db, colName, 1000, "Alvalade", "Filmagem");

            //2.e: range query (Feb 2017)
            LabOperations.getEventsInFeb2017(db, colName);

            //2.f: multi-field range query
            LabOperations.getEventsBetweenDates(db, colName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}