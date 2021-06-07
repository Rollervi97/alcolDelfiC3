from py4j.java_gateway import JavaGateway
from py4j.java_gateway import launch_gateway
from py4j.protocol import Py4JJavaError
import os
import sys
from datetime import datetime
import psycopg2


def getTab(tabname, fieldlist, fieldtype, notnulllist,PKindex):

    if not len(fieldlist) == len(fieldtype) or not len(fieldlist) == len(notnulllist) or PKindex<0 or PKindex>len(notnulllist):
        print("command not executed - lenght of required input not correct")
        return " "

    tabgenq = "CREATE TABLE public."+tabname + " ( "
    for i in range(len(fieldlist)):
        tabgenq += fieldlist[i].replace(".", "_")+" "+fieldtype[i]
        if i == PKindex:
            tabgenq += " PRIMARY KEY "
        if notnulllist[i] and not i == PKindex:
            tabgenq += " NOT NULL,"

        else:
            tabgenq += ","


    tabgenq = tabgenq[:-1]
    tabgenq += ");"
    return tabgenq


def process_frame(stream, data):
    model = stream.processStream(data)
    entries = model.getContentList()
    print("Entering process frame func")
    #
    val_list = []
    name_list = []
    validity = []
    meas_unit = []
    for entry in entries:
        val = entry.getValue()

        name = entry.getName()
        name_list.append(name)



        if not val:
            # print(name, " empty value\n")
            val_list.append(None)
            meas_unit.append(None)
        else:
            print(name, " ",val.getCalibratedValue(), " ", entry.getParameter().getUnits(), " (", val.getRawValueHex(), ") ")

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


if __name__ == '__main__' :

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
    CONNECTION = "dbname=alcoldatabase user=alcol password=alcolpassword host=127.0.0.1 port=5432"
    dbconn = psycopg2.connect(CONNECTION)
    cur = dbconn.cursor()


    debugrec = open("debug_rec_create_tab.txt", 'w')
    path = os.getcwd()

    file = "Delfi-C3_simplified.xml"
    # file = "Delfi-C3.xml"
    fp = path+ "\\"+ file
    print(os.path.isfile(fp))
    if not os.path.isfile(fp):
        sys.error("XML XTCE file not found")


    db_ = XTCEDatabase(File(fp), True, False, True)

    containerList, containerObj = getContainerList(db_, debugrec)
    tabcheck = []
    for i in range(len(containerList)):
        tabcheck.append(False)


    pathlog = 'C:\\Users\\ASUS\\Desktop\\LUNAR_ZEBRO\\DELFI\\PyTrack\\PyTrack'
    filenamelist = os.listdir(pathlog)
    #
    succ = 0
    # reading each frame from each file
    for namelog in filenamelist:
        if namelog[-4:] == ".log": # reading each ".log" file
            logname = pathlog + '\\' + namelog
            f = open(logname)
            for line in f.readlines(): # reading each line/frame of the file
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

                try:

                    db_ = XTCEDatabase(File(fp), True, False, True)
                    warns = db_.getDocumentWarnings()
                    for w in warns:
                        print("ERROR: ", w)

                        debugrec.write(namelog[:-4], " ERROR ", w )
                    print("About to execute process_frame, the total frames processed from this file is: \n")
                    stream = db_.getStream("TLM")
                    name, values, valid, MU = process_frame(stream, hexb) #define useful output for this function

                    for kk in range(len(containerList)):
                        if not tabcheck[kk] and values[5] == containerList[kk]:
                            name.insert(0, "time")
                            ftype = ["text"] * len(name)
                            ftype[0] = "timestamp"
                            nll = [0] * len(name)
                            smt = getTab(containerList[kk], name, ftype, nll, 0)
                            tabcheck[kk] = True
                            cur.execute(smt)
                            dbconn.commit()
                    cout = 0
                    for i in range(len(tabcheck)):
                        if tabcheck[i]:
                            cout +=1

                    if cout == len(tabcheck):
                        sys.exit("All the table have been written properly")
                    succ += 1
                except Py4JJavaError as ex:
                    print("Some unknown exception raised", ex.errmsg)
                    # print("Some unknown exception raised", ex.errmsg, ex.java_exception)
                    # strerr = namelog[:-4]+ " "+str(linec)+ " EXCEPTION " + ex.errmsg
                    # debugrec.write(strerr)

        print('Successfully parsed frames = ', succ)
