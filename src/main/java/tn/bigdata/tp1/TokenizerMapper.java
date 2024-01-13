package tn.bigdata.tp1;

/*import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
public class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

       /* private final static DoubleWritable one = new DoubleWritable();
        private Text word = new Text();
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }

    }

    private Text word = new Text();
    private DoubleWritable one = new DoubleWritable();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length >= 6) {
            word.set(fields[2]);
            try {
                double amount = Double.parseDouble(fields[4]);
                one.set(amount);
                context.write(word, one);
            } catch (NumberFormatException e) {
                // Handle parsing error
                e.printStackTrace();
            }
        }
    }
}*/
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

   //Variable de stockage du nom de magasin
    private Text store = new Text();

    //Variable du stockage du montant de la vente
    private DoubleWritable saleAmount = new DoubleWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        //Extraction des données avec la méthode "split",
        // en utilisant la séparation "\\s+" correspondant à plusieurs espaces séparant les differents champs
        String[] fields = value.toString().split(" \\s+");

        //Condition de vérification de l'existence des champs
        if (fields.length >= 6) {

            // Extraction et set du nom de magasin (champ2) et du montant de vente (champ4)
            String storeName = fields[2];

            try {

                //Conversion du montant (amount) en un nombre double
                double amount = Double.parseDouble(fields[4]);
                store.set(storeName);
                saleAmount.set(amount);
                //Paire clé-valeur envoyée au reducer
                context.write(store, saleAmount);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing amount for store: " + storeName);
            }
        }
    }
}



