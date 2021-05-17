from py4j.java_gateway import JavaGateway
import os
import sys


def isWithinValidRange(entry):
    param = entry.getParameter()

    if not param:
        return True

    rang = param.getValidRange()


    if not rang.isValidRangeApplied():
        return True
    else:

        if rang.isLowValueCalibrated():
            valLow = entry.getValue().getCalibratedValue()
        else:
            valLow = entry.getValue().getUncalibratedValue()


        if rang.isLowValueInclusive():
            if float(valLow) < float(rang.getLowValue()):
                return False
        else:
            if float(valLow) <= float(rang.getLowValue()):
                return False

        if rang.isHighValueCalibrated():
            valHigh = entry.getValue().getCalibratedValue()
        else:
            valHigh = entry.getValue().getUncalibratedValue()

        if rang.isHighValueInclusive():
            if float(valHigh) <  float(rang.getHighValue()):
                return False
        else:
            if float(valHigh) <=  float(rang.getHighValue()):
                return False

        return True



def process_frame(stream, data):
    print("start process_frame function")
    model = stream.processStream(data)
    entries = model.getContentList()
    #
    # print("type model", type(model))
    # print("entries", type(entries))

    for entry in entries:
        val = entry.getValue()
        name = entry.getName()

        if not val:
            print(name, " empty value\n")
        else:
            print(name, " ",val.getCalibratedValue(), " ", entry.getParameter().getUnits(), " (", val.getRawValueHex(), ") ")


        if not isWithinValidRange(entry):
            print("INVALID!! \n")
        else:
            print("\n")




if __name__ == '__main__' :

    gateway = JavaGateway()

    main = gateway.jvm.me.cllsll.alcol.Main()

    XTCEContainerContentModel = gateway.jvm.org.xtce.toolkit.XTCEContainerContentModel
    XTCEContainerEntryValue = gateway.jvm.org.xtce.toolkit.XTCEContainerEntryValue
    XTCEDatabase = gateway.jvm.org.xtce.toolkit.XTCEDatabase
    XTCEDatabaseException = gateway.jvm.org.xtce.toolkit.XTCEDatabaseException
    XTCEParameter = gateway.jvm.org.xtce.toolkit.XTCEParameter
    XTCETMStream = gateway.jvm.org.xtce.toolkit.XTCETMStream
    XTCEValidRange = gateway.jvm.org.xtce.toolkit.XTCEValidRange
    File = gateway.jvm.java.io.File
    Iterator = gateway.jvm.java.util.Iterator
    List = gateway.jvm.java.util.List


    # define a function that reads database binary thing one by one
    hk = main.bytehk()
    p = main.bytep()
    # print(main.bytehk())


    print(hk)

    # in static void main in the java code there is:
    # definition of test messages, here achieved using "main.bytehk()" and "main.bytep()"
    path = os.getcwd()

    file = "Delfi-C3.xml"
    fp = path+ "\\"+ file
    print(os.path.isfile(fp))
    if not os.path.isfile(fp):
        sys.error("XML XTCE file not found")

    db_ = XTCEDatabase(File(fp), True, False, True)
    # print(type(db_))
    stream = db_.getStream("TLM")
    process_frame(stream, hk)
    # print(type(stream))
    #
    # main.execute(fp, hk)


