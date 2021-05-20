from py4j.java_gateway import JavaGateway
from py4j.java_gateway import launch_gateway
from py4j.protocol import Py4JJavaError
import os
import sys
from datetime import datetime
import psycopg2


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



def process_frame(stream, data): # maybe add values field
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

def getContainerList (database, debugfile):
    s = database.getStreams().iterator()
    containers = []
    while s.hasNext():
        ic = s.next().getContainers().iterator()
        print(ic.hasNext())
        while ic.hasNext():
            cc = ic.next()
            print(cc.isAbstract())
            if not cc.isAbstract():
                print("before updating container list")

                try:
                    # temp1 = db_.getSpaceSystemTree()
                    # print(type(temp1), temp1)
                    # temp2 = XTCEContainerContentModel(cc, temp1, None, False)
                    # print(type(temp2), temp2)
                    containers.append(XTCEContainerContentModel(cc, db_.getSpaceSystemTree(), None, False).getName())
                except Py4JJavaError as ex:
                    print("Some unknown exception raised", ex.errmsg)
                    debugfile.write("EXECPTION while reading containers names", ex.errmsg )
    return containers



if __name__ == '__main__' :

    # launch_gateway(classpath='org.xtce.toolkit.*')
    launch_gateway(jarpath='..\\java\\out\\artifacts\\XTCEJtoPy_jar\\XTCEJtoPy.jar',
                   classpath='me.xtce.jtopy.Main',
                   port=25333,
                   die_on_exit=True)

    gateway = JavaGateway()

    XTCEContainerContentModel = gateway.jvm.org.xtce.toolkit.XTCEContainerContentModel
    XTCEContainerEntryValue = gateway.jvm.org.xtce.toolkit.XTCEContainerEntryValue
    XTCEDatabase = gateway.jvm.org.xtce.toolkit.XTCEDatabase
    XTCEDatabaseException = gateway.jvm.org.xtce.toolkit.XTCEDatabaseException
    XTCEParameter = gateway.jvm.org.xtce.toolkit.XTCEParameter
    XTCETMStream = gateway.jvm.org.xtce.toolkit.XTCETMStream
    XTCEValidRange = gateway.jvm.org.xtce.toolkit.XTCEValidRange
    XTCEContainer = gateway.jvm.org.xtce.toolkit.XTCETMContainer
    XTCEEngineeringType = gateway.jvm.org.xtce.toolkit.XTCETypedObject.EngineeringType

    File = gateway.jvm.java.io.File
    Iterator = gateway.jvm.java.util.Iterator
    List = gateway.jvm.java.util.List

    # setting up tiemscaledb database
    """
    CONNECTION = "dbname=tsdb user=tsdbadmin password=secret host=host.com port=5432 sslmode=require"
    dbconn = psycopg2.connect(CONNECTION)
    insert_data(dbconn)
    cur = dbconn.cursor()

    timescaledb quick tutorial
    query_create_newtable = "CREATE TABLE table_name (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50))"
    # execute statement 
    # NOTE: better to create a class with database infos and methods to create tables and other commands
    cur.execute(query_create_newtable)
    dbconn.commit()
    # cur.close()
    
    
    """
    debugrec = open("debug_rec.txt", 'w')
    path = os.getcwd()
    file = "Delfi-C3.xml"
    fp = path+ "\\"+ file
    print(os.path.isfile(fp))
    if not os.path.isfile(fp):
        sys.error("XML XTCE file not found")


    db_ = XTCEDatabase(File(fp), True, False, True)

    containerList = getContainerList(db_, debugrec)


    print(containerList)

    print("End of container test")
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # define a function that reads database binary thing one by one


    pathlog = 'C:\\Users\\ASUS\\Desktop\\LUNAR_ZEBRO\\DELFI\\PyTrack\\PyTrack'
    filenamelist = os.listdir(pathlog)
    #
    succ = 0
    for namelog in filenamelist:
        if namelog[-4:] == ".log":
            logname = pathlog + '\\' + namelog
            f = open(logname)
            linec = 0
            for line in f.readlines():
                linec += 1
                ss = line.split(',',-1)
                if len(ss):
                    t = ss[0]
                    freq = ss[1]
                    hexs = ss[2]
                else:
                    t = ss[0]
                    hexs = ss[1]


                date = datetime.strptime(t[:-4], '%Y-%m-%d %H:%M:%S.%f')
                hexb = bytes.fromhex(hexs)

                try:

                    db_ = XTCEDatabase(File(fp), True, False, True)
                    warns = db_.getDocumentWarnings()
                    for w in warns:
                        print("ERROR: ", line)
                        debugrec.write(namelog[:-4], " ",linec, " ERROR ", w )

                    stream = db_.getStream("TLM")
                    process_frame(stream, hexb)

                    succ += 1
                except Py4JJavaError as ex:
                    print("Some unknown exception raised", ex.errmsg)
                    debugrec.write(namelog[:-4], " ",linec, " EXCEPTION ", ex.errmsg )

        print('Successfully parsed frames = ', succ)

    # after this just random testing crap       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    namelog = '20190130_090441z_145868400_32789_packets.log'
    logname = 'C:\\Users\\ASUS\\Desktop\\LUNAR_ZEBRO\\DELFI\\PyTrack\\PyTrack\\20190130_090441z_145868400_32789_packets.log'
    logname = pathlog + '\\' + namelog
    f = open(logname)
    splitstring= f.readline().split(',',-1)
    print(type(splitstring), splitstring, len(splitstring))


    lengthlist = []

    line = f.readline()
    t,freq, hexstr = line.split(',', 3)
    hexbyte = bytes.fromhex(hexstr)


    # print(type(t), t[:-4])
    date = datetime.strptime(t[:-4], '%Y-%m-%d %H:%M:%S.%f')
    # print(type(date), date)
    hexbyte = bytes.fromhex(hexstr)
    print(sys.getsizeof(hexbyte), hexbyte)

    # in static void main in the java code there is:
    # definition of test messages, here achieved using "main.bytehk()" and "main.bytep()"
    path = os.getcwd()

    file = "Delfi-C3.xml"
    fp = path+ "\\"+ file
    print(os.path.isfile(fp))
    if not os.path.isfile(fp):
        sys.error("XML XTCE file not found")

    try:

        db_ = XTCEDatabase(File(fp), True, False, True)
        warns = db_.getDocumentWarnings()
        for line in warns:
            print("ERROR: ", line)

        stream = db_.getStream("TLM")
        # try to get container list



        process_frame(stream, hexbyte)
        # print(type(stream))
        # main.execute(fp, hk)
    except Py4JJavaError as ex:
        print("Some unknown exception raised", ex.errmsg)


    print("out of the try except loop")





