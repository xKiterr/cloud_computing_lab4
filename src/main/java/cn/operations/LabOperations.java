package cn.operations;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class LabOperations {

    public static OcupacaoTemporaria convertLineToObject(String line) throws ParseException {
        String[] cols = line.split(",");
        OcupacaoTemporaria ocup = new OcupacaoTemporaria();
        ocup.ID = Integer.parseInt(cols[0]);
        ocup.location = new Localizacao();
        ocup.location.point = new GeoPoint(Double.parseDouble(cols[1]), Double.parseDouble(cols[2]));
        ocup.location.coord = new Coordenadas();
        ocup.location.coord.X = Double.parseDouble(cols[1]);
        ocup.location.coord.Y = Double.parseDouble(cols[2]);
        ocup.location.freguesia = cols[3];
        ocup.location.local = cols[4];
        ocup.event = new Evento();
        ocup.event.evtID = Integer.parseInt(cols[5]);
        ocup.event.nome = cols[6];
        ocup.event.tipo = cols[7];
        ocup.event.details = new HashMap<String, String>();
        if (!cols[8].isEmpty()) ocup.event.details.put("Participantes", cols[8]);
        if (!cols[9].isEmpty()) ocup.event.details.put("Custo", cols[9]);
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        ocup.event.dtInicio = formatter.parse(cols[10]);
        ocup.event.dtFinal = formatter.parse(cols[11]);
        ocup.event.licenciamento = new Licenciamento();
        ocup.event.licenciamento.code = cols[12];
        ocup.event.licenciamento.dtLicenc = formatter.parse(cols[13]);
        return ocup;
    }

    public static void insertDocuments(String pathnameCSV, Firestore db, String collectionName) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(pathnameCSV));
        CollectionReference colRef = db.collection(collectionName);
        String line;
        while ((line = reader.readLine()) != null) {
            OcupacaoTemporaria ocup = convertLineToObject(line);
            DocumentReference docRef = colRef.document("Lab4-" + ocup.ID);
            ApiFuture<WriteResult> resultFut = docRef.set(ocup);
            WriteResult result = resultFut.get();
            System.out.println("Update time : " + result.getUpdateTime());
        }
    }

    //2.a: present the content of a document based on its identifier
    public static void printDocumentById(Firestore db, String collectionName, String docId) throws Exception {
        DocumentReference docRef = db.collection(collectionName).document(docId);
        ApiFuture<DocumentSnapshot> future = docRef.get();
        DocumentSnapshot document = future.get();

        if (document.exists()) {
            System.out.println("2a. Document data: " + document.getData());
        } else {
            System.out.println("2a. No such document");
        }
    }

    //2.b: delete a field from a document
    public static void deleteFieldFromDocument(Firestore db, String collectionName, String docId, String fieldPath) throws Exception {
        DocumentReference docRef = db.collection(collectionName).document(docId);
        ApiFuture<WriteResult> writeResult = docRef.update(fieldPath, FieldValue.delete());
        System.out.println("2b. Field '" + fieldPath + "' deleted at: " + writeResult.get().getUpdateTime());
    }

    //2.c: simple interrogation for all documents of a given parish
    public static void getDocumentsByParish(Firestore db, String collectionName, String parish) throws Exception {
        ApiFuture<QuerySnapshot> future = db.collection(collectionName)
                .whereEqualTo("location.freguesia", parish)
                .get();

        List<QueryDocumentSnapshot> documents = future.get().getDocuments();
        System.out.println("2c. Found " + documents.size() + " events in " + parish + ":");
        for (DocumentSnapshot doc : documents) {
            System.out.println(" - " + doc.getId());
        }
    }

    //2.d: compound interrogation
    public static void getCompoundQuery(Firestore db, String collectionName, int minId, String parish, String eventType) throws Exception {
        Query query = db.collection(collectionName)
                .whereGreaterThan("ID", minId)
                .whereEqualTo("location.freguesia", parish)
                .whereEqualTo("event.tipo", eventType);

        List<QueryDocumentSnapshot> documents = query.get().get().getDocuments();
        System.out.println("2d. Found " + documents.size() + " documents matching criteria.");
    }

    //2.e: interrogation to obtain the documents with events that started in Feb 2017
    public static void getEventsInFeb2017(Firestore db, String collectionName) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date startDate = sdf.parse("31/01/2017");
        Date endDate = sdf.parse("01/03/2017");

        Query query = db.collection(collectionName)
                .whereGreaterThan("event.dtInicio", startDate)
                .whereLessThan("event.dtInicio", endDate);

        List<QueryDocumentSnapshot> documents = query.get().get().getDocuments();
        System.out.println("2e. Events started in Feb 2017: " + documents.size());
    }

    //2.f: interrogation for events carried out between two dates (Multi-field range)
    public static void getEventsBetweenDates(Firestore db, String collectionName) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date startDate = sdf.parse("31/01/2017");
        Date endDate = sdf.parse("01/03/2017");

        Query query = db.collection(collectionName)
                .whereGreaterThan("event.dtInicio", startDate)
                .whereLessThan("event.dtFinal", endDate);

        List<QueryDocumentSnapshot> documents = query.get().get().getDocuments();
        System.out.println("2f. Events carried out between dates: " + documents.size());
    }
}