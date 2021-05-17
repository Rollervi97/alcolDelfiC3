package me.cllsll.alcol;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import org.xtce.toolkit.XTCEContainerContentEntry;
import org.xtce.toolkit.XTCEContainerContentModel;
import org.xtce.toolkit.XTCEContainerEntryValue;
import org.xtce.toolkit.XTCEDatabase;
import org.xtce.toolkit.XTCEDatabaseException;
import org.xtce.toolkit.XTCEParameter;
import org.xtce.toolkit.XTCETMStream;
import org.xtce.toolkit.XTCEValidRange;
import py4j.GatewayServer;

public class Main {

//    public String sayHello() {
//        return "HEllo mf";
//    }
    public void execute(String file, byte[] data){
        try {

            XTCEDatabase db_ = new XTCEDatabase(new File(file), true, false, true);
            XTCETMStream stream = db_.getStream( "TLM" );

            //processFrame(stream, p);
            processFrame(stream, data);
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }


    }

    public byte[] bytehk() {

        return new byte[]{(byte) 0xA8, (byte) 0x98, (byte) 0x9A, (byte) 0x40, (byte) 0x40, (byte) 0x40,
                (byte) 0x00, (byte) 0x88, (byte) 0x98, (byte) 0x8C, (byte) 0x92, (byte) 0x86,
                (byte) 0x66, (byte) 0x01, (byte) 0x03, (byte) 0xF0, (byte) 0xE1, (byte) 0x08,
                (byte) 0xFA, (byte) 0x01,
                (byte) 0xDE, (byte) 0x84, (byte) 0xF4, (byte) 0xFF,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF, (byte) 0x3F,
                (byte) 0x97, (byte) 0x96, (byte) 0x55, (byte) 0x00, (byte) 0x00, (byte) 0x1F,
                (byte) 0xB6,
                (byte) 0xC0, (byte) 0x00, (byte) 0x20, (byte) 0x02, (byte) 0x00,
                (byte) 0x7E, (byte) 0x3C, (byte) 0x76, (byte) 0x07, (byte) 0x00,
                (byte) 0xD5, (byte) 0x00, (byte) 0x80, (byte) 0x02, (byte) 0x00, (byte) 0xD4, (byte) 0x03,
                (byte) 0x40, (byte) 0x18, (byte) 0x00, (byte) 0x90, (byte) 0x01, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x5E, (byte) 0x7F,
                (byte) 0x0A, (byte) 0x8D};
    }
    public byte[] bytep() {
        return new byte[]{(byte) 0xA8, (byte) 0x98, (byte) 0x9A, (byte) 0x40, (byte) 0x40, (byte) 0x40,
                (byte) 0x00, (byte) 0x88, (byte) 0x98, (byte) 0x8C, (byte) 0x92, (byte) 0x86,
                (byte) 0x66, (byte) 0x01, (byte) 0x03, (byte) 0xF0, (byte) 0xE1, (byte) 0x08,
                (byte) 0xF7, (byte) 0x01, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xA5, (byte) 0xB2, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x5E, (byte) 0x7F, (byte) 0x0A,
                (byte) 0x8A, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
    }

    public static void main(String[] args) {
        Main app = new Main();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        server.start();
    }

    static void processFrame(XTCETMStream stream, byte[] data) throws XTCEDatabaseException, Exception
    {
        XTCEContainerContentModel model = stream.processStream( data );

        List<XTCEContainerContentEntry> entries = model.getContentList();

        for (XTCEContainerContentEntry entry : entries) {
            System.out.print(entry.getName());

            XTCEContainerEntryValue val = entry.getValue();


            if (val == null)
            {
                System.out.println();
            } else
            {
                System.out.print(": " + val.getCalibratedValue() + " ["
                        + entry.getParameter().getUnits() + "] ("
                        + val.getRawValueHex()+ ")");

                if (!isWithinValidRange(entry))
                {
                    System.out.println(" INVALID!");
                }
                else
                {
                    System.out.println();
                }
            }
        }
        List<String> warnings = model.getWarnings();
        Iterator<String> it = warnings.iterator();
        while(it.hasNext())
        {
            System.err.println("WARNING: " + it.next());
        }
        System.out.println();
    }

    static private boolean isWithinValidRange(XTCEContainerContentEntry entry)
    {
        XTCEParameter param = entry.getParameter();
        XTCEValidRange range = param.getValidRange();
        if (!range.isValidRangeApplied()) {
            return true;
        } else {
            String valLow =  range.isLowValueCalibrated() ?
                    entry.getValue().getCalibratedValue() :
                    entry.getValue().getUncalibratedValue();

            if (range.isLowValueInclusive()) {
                if (Double.parseDouble(valLow) < Double.parseDouble(range.getLowValue())) {
                    return false;
                }
            } else {
                if (Double.parseDouble(valLow) <= Double.parseDouble(range.getLowValue())) {
                    return false;
                }
            }

            String valHigh =  range.isHighValueCalibrated() ?
                    entry.getValue().getCalibratedValue() :
                    entry.getValue().getUncalibratedValue();

            if (range.isHighValueInclusive()) {
                if (Double.parseDouble(valHigh) > Double.parseDouble(range.getHighValue())) {
                    return false;
                }
            } else {
                if (Double.parseDouble(valHigh) >= Double.parseDouble(range.getHighValue())) {
                    return false;
                }
            }
        }
        return true;
    }
}
