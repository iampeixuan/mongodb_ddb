import pymongo
import datetime


def xact_T(database, xact):
    t1 = datetime.datetime.now()
    print("========== TOP-BALANCE TRANSACTION ==========")
    print("Top-10 customer with highest c_balance:")
    count = 1
    projection = {"c_first", "c_middle", "c_last", "w_name", "d_name", "c_balance"}
    it = database.collection2.find({}, projection).sort("c_balance", 1).limit(10)
    for cus in it:
        tuple = (count, cus["c_first"], cus["c_middle"], cus["c_last"], cus["w_name"], cus["d_name"], cus["c_balance"])
        print("%2d. Customer %s %s %s in warehouse %s district %s has outstanding balance of %s" % tuple)
        count += 1

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'T', latency
