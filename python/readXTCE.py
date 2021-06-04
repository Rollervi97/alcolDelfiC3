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

def getSamp1():
    a = b'\xA8\x98\x9A\x40\x40\x40\x00\x88\x98\x8C\x92\x86\x66\x01\x03\xF0\xE1\x08\xFA\x01\xDE\x84\xF4\xFF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xFF\x3F\x97\x96\x55\x00\x00\x1F\xB6\xC0\x00\x20\x02\x00\x7E\x3C\x76\x07\x00\xD5\x00\x80\x02\x00\xD4\x03\x40\x18\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x5E\x7F\x0A\x8D'
    return a

# def getSamp2():



def process_frame(stream, data):
    # print(data)
    model = stream.processStream(data)
    entries = model.getContentList()
    print("Entering process frame func")
    # print("type model", type(model))
    # print("entries", type(entries))
    val_list = []
    name_list = []
    validity = []
    meas_unit = []
    # print(type(entries), len(entries))
    for entry in entries:
        val = entry.getValue()

        name = entry.getName()
        name_list.append(name)

        if name == "FrameID":
            print(name, " ",val.getCalibratedValue(), " ", entry.getParameter().getUnits(), " (", val.getRawValueHex(), ") ")



        if not val:
            # print(name, " empty value\n")
            val_list.append(None)
            meas_unit.append(None)
        else:
            # print(name, " ",val.getCalibratedValue(), " ", entry.getParameter().getUnits(), " (", val.getRawValueHex(), ") ")

            val_list.append(val.getCalibratedValue())
            meas_unit.append(entry.getParameter().getUnits())

        if not isWithinValidRange(entry):
            validity.append(0)
            # print("INVALID!! \n")
        else:
            validity.append(1)
            # print("\n")


    return name_list, val_list, validity, meas_unit


def getContainerList (database, debugfile):
    s = database.getStreams().iterator()
    containersname = []
    containersobj = []
    while s.hasNext():
        ic = s.next().getContainers().iterator()

        while ic.hasNext():
            cc = ic.next()

            if not cc.isAbstract():


                try:
                    # temp1 = db_.getSpaceSystemTree()
                    # print(type(temp1), temp1)
                    # temp2 = XTCEContainerContentModel(cc, temp1, None, False)
                    # print(type(temp2), temp2)
                    containersname.append(XTCEContainerContentModel(cc, db_.getSpaceSystemTree(), None, False).getName())
                    containersobj.append(XTCEContainerContentModel(cc, db_.getSpaceSystemTree(), None, False))
                except Py4JJavaError as ex:
                    print("Some unknown exception raised", ex.errmsg)
                    debugfile.write("EXECPTION while reading containers names", ex.errmsg )
    return containersname, containersobj



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


    """
    Steps:
    (1) Read containers and relative fields. Check if tables already exist, if not Create tables accordingly.
    (2) Read files row by row and write each frame in the proper table.
    """


    # setting up tiemscaledb database
    # CONNECTION = "dbname=alcoldatabase user=alcol password=alcolpassword host=127.0.0.1 port=5432"
    # dbconn = psycopg2.connect(CONNECTION)
    # cur = dbconn.cursor()
    #
    #
    # query_create_newtable = "CREATE TABLE alcoltab1 ();"
    # print(type(dbconn))
    # print(query_create_newtable)
    # cur.execute(query_create_newtable)
    #
    # s = "SELECT"
    # s += " table_schema"
    # s += ", table_name"
    # s += " FROM information_schema.tables"
    # s += " WHERE "
    # s += "("
    # s += " table_schema = 'public'"
    # s += ")"
    # s += " ORDER BY table_name;"
    #
    # tablist = cur.execute("select * from _timescaledb_catalog.hypertable;")
    # print(type(tablist), tablist)
    #
    # tablist = cur.execute(s)
    # print(type(tablist), tablist)
    """
    
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
    samp1 = getSamp1()
    print(samp1)
    print(type(samp1))
    file = "Delfi-C3_simplified.xml"
    # file = "Delfi-C3.xml"
    fp = path+ "\\"+ file
    print(os.path.isfile(fp))
    if not os.path.isfile(fp):
        sys.error("XML XTCE file not found")


    db_ = XTCEDatabase(File(fp), True, False, True)
    # stream = db_.getStream("TLM")
    # name, values, valid, MU = process_frame(stream, samp1)
    # print("printing namelist \n",name)
    # print("printing value \n",values)
    # print("printing validity \n",valid)
    # print("printing meas unit \n",MU)
    # if not len(name)==len(values) or not len(valid)==len(values) or not len(valid)==len(MU):
    #     print("printing namelist \n",len(name))
    #     print("printing value \n",len(values))
    #     print("printing validity \n",len(valid))
    #     print("printing meas unit \n",len(MU))
    #     sys.exit("list length not correct")
    containerList, containerObj = getContainerList(db_, debugrec)

    lala1 = containerObj[1]
    lala2 = lala1.getContentList()
    contc = 0
    contit = 0

    print(lala2.toString())
    f = True
    for cont in containerObj:
        contc +=1
        for it in cont.getContentList():

            contit += 1
            la1 = cont.getName()
            la2 = it.getName()
            la3 = it
            la4 = it.getValue()
            if la4:
                print("Value changed")
                la5 = la4.getCalibratedValue()
            else:
                print("value not changed")
            print(la1, "---" , la2, "\n")




    print("tot container %f, tot fields %f", contc, contit)

    print(containerList)

    print("End of container test")


    pathlog = 'C:\\Users\\ASUS\\Desktop\\LUNAR_ZEBRO\\DELFI\\PyTrack\\PyTrack'
    filenamelist = os.listdir(pathlog)
    #
    succ = 0
    # reading each frame from each file
    for namelog in filenamelist:
        if namelog[-4:] == ".log": # reading each ".log" file
            logname = pathlog + '\\' + namelog
            f = open(logname)
            linec = 0
            for line in f.readlines(): # reading each line/frame of the file
                linec += 1
                ss = line.split(',',-1)
                print(len(ss))
                if len(ss) == 3:
                    t = ss[0]
                    freq = ss[1]
                    hexs = ss[2]
                else:
                    t = ss[0]
                    hexs = ss[1]


                date = datetime.strptime(t[:-4], '%Y-%m-%d %H:%M:%S.%f')
                hexb = bytes.fromhex(hexs)
                # print(hexs)
                # print("-> ", hexb)
                # print("ready to open xtce database and get values")
                try:

                    db_ = XTCEDatabase(File(fp), True, False, True)
                    warns = db_.getDocumentWarnings()
                    for w in warns:
                        print("ERROR: ", w)

                        debugrec.write(namelog[:-4], " ",linec, " ERROR ", w )

                    stream = db_.getStream("TLM")
                    name, values, valid, MU = process_frame(stream, hexb) #define useful output for this function
                    # print("printing namelist \n",name)
                    # print("printing value \n",values)
                    # print("printing validity \n",valid)
                    # print("printing meas unit \n",MU)
                    succ += 1
                except Py4JJavaError as ex:
                    print("Some unknown exception raised", ex.errmsg, ex.java_exception)
                    strerr = namelog[:-4]+ " "+str(linec)+ " EXCEPTION " + ex.errmsg
                    debugrec.write(strerr)

        print('Successfully parsed frames = ', succ)

    # after this just random testing crap       %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    """
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
"""




