// Daniel Pitzl, DESY, Nov 2024
// print .pb file in TSV format

// javac pb2parquet.java
// java pb2parquet CUR_AVERAGE:2024_11_05.pb > CUR_AVERAGE-2024-11-05.tsv
//
// you can give several .pb files as input
// they are chained into one .tsv file
// with a common starting timestamp in ms
//
// multi-valued PVs have all values written out

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.sql.Timestamp;
import java.time.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.json.simple.JSONObject;

// from https://mvnrepository.com/artifact/org.epics/epics-pvdata
import org.epics.pvdata.pv.PVStructure;

// https://mvnrepository.com/artifact/org.epics/jca

import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;
//import org.json.JSONObject;
import org.json.XML;
import org.json.JSONException;

// from https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java/4.28.3
import com.google.protobuf.MessageOrBuilder;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;

public class pb2parquet {

    @SuppressWarnings("unchecked")

    // --- Helpers added ---
    private static String escapeTSV(String s) {
        return s == null ? "" : s.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static void printSampleValue(SampleValue theval) {
        int n = theval.getElementCount();

        if (n <= 1) {
            // Try numeric/scalar access; if unsupported (e.g. scalar string), fall back to toString()
            try {
                Object v0 = theval.getValue(0);
                if (v0 instanceof CharSequence) {
                    System.out.print(" \"" + escapeTSV(v0.toString()) + "\"");
                } else {
                    System.out.print(" " + v0);
                }
            } catch (UnsupportedOperationException uoe) {
                // scalar string PVs land here
                String s = theval.toString();
                System.out.print(" \"" + escapeTSV(s) + "\"");
            }
        } else {
            // Vector: element-wise, with a graceful fallback if the vector doesn't support getValue(i)
            for (int i = 0; i < n; ++i) {
                try {
                    Object vi = theval.getValue(i);
                    if (vi instanceof CharSequence) {
                        System.out.print(" \"" + escapeTSV(vi.toString()) + "\"");
                    } else {
                        System.out.print(" " + vi);
                    }
                } catch (UnsupportedOperationException uoe) {
                    // As a last resort, print the whole thing once
                    String s = theval.toString();
                    System.out.print(" \"" + escapeTSV(s) + "\"");
                    break;
                }
            }
        }
    }
    // --- End helpers ---

    public static void main( String[] args ) throws Exception {

        if( args == null || args.length < 1 ) {
            System.err.println( "Usage: pb2parquet <PBFiles>" );
            return;
        }

        // Fixed anchor: 2024-01-01 00:00:00 JST (UTC+9)
        long t2024 = 1704034800L; // [s]
        long t2024ms = 1000L * t2024; // [ms]

        // Optional alternative anchor kept for reference:
        // long t202411 = 1730419200L; // [s] 1.11.2024 00:00:00 UTC
        // long t202411ms = 1000L * t202411; // [ms]

        long ts0 = 0; // first time stamp across all inputs
        int nev = 0;

        // loop over .pb files in argument list:
        for( String fileName : args ) {

            System.out.println( "# file " + fileName );

            Path path = Paths.get(fileName);
            PBFileInfo info = new PBFileInfo(path);

            try( FileBackedPBEventStream strm =
                 new FileBackedPBEventStream( info.getPVName(), path, info.getType()) ) {

                System.out.println( "# PV name " + info.getPVName() );

                for( Event ev : strm ) {

                    // https://epicsarchiver.readthedocs.io/en/latest/_static/javadoc/org/epics/archiverappliance/Event.html
                    //System.out.print( ev.getEpochSeconds() );

                    Timestamp thetime = ev.getEventTimeStamp();
                    SampleValue theval = ev.getSampleValue(); // scalar or vector

                    if( nev == 0 ) {
                        ts0 = thetime.getTime(); // [ms]
                        System.out.println( "# Fixed anchor: 2024-01-01 00:00:00 JST (UTC+9)" );
                        System.out.println( "# t0 " + t2024ms ); // [ms] since fixed anchor
                        System.out.println( "# Nval " + theval.getElementCount() );
                    }

                    System.out.print( thetime.getTime() - t2024ms ); // [ms] since fixed anchor

                    //System.out.print( " " );
                    //System.out.print( thetime.getNanos() ); // redundant?

                    // Safe printing for numeric, string, scalar, and vector PVs
                    printSampleValue(theval);

                    System.out.println(); // endl
                    ++nev;

                } // ev in strm

                System.err.print( fileName );
                System.err.print( "  " + info.getPVName() );
                System.err.print( "  lines " );
                System.err.print( nev );
                System.err.println(); // endl

            } // try

        } // files

    } // main
}