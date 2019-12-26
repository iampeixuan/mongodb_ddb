import pymongo
import datetime


def xact_R(database, xact):
    t1 = datetime.datetime.now()
    print("========== RELATED-CUSTOMER TRANSACTION ==========")
    ids = xact.rstrip().split(',')

    w_id = int(ids[1])
    d_id = int(ids[2])
    c_id = int(ids[3])
    print("Finding customers related to customer %d in warehouse %d district %d:" % (c_id, w_id, d_id))

    orders_array = database.collection2.find_one({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"items_ordered"})["items_ordered"]
    items_ordered = set() #all items ordered by the customer
    orders_items = [] #list of orders, each element in list containing a set of item_ids
    for order in orders_array:
        items = set()
        orders_items.append(items)
        for i_id in order["items"]:
            items_ordered.add(i_id)
            items.add(i_id)

    items_ordered = list(items_ordered)
    # Get customers in another warehouse that has at least 1 common item ordered
    projection = {"c_id", "items_ordered"}
    it = database.collection2.find({"w_id": {"$ne": w_id}, "items_ordered": {"$elemMatch": {"items": {"$elemMatch": {"$in": items_ordered}}}}}, projection)
    #print(len(list(it))) # check count of customers that has at least 1 common item
    #print(database.collection2.count_documents({"w_id": {"$ne": w_id}})) # check count of all customer not in the same warehouse

    count = 1
    for customer in it:
        cus2_orders_items = []
        for order in customer["items_ordered"]:
            cus2_orders_items.append(set(order["items"]))
        if has_mt2_common(orders_items, cus2_orders_items):
            print("%3d. Customer %s has at least 2 items in common" % (count, customer["c_id"]))
            count += 1

    t2 = datetime.datetime.now()
    latency = (t2-t1).seconds * 1000 + (t2-t1).microseconds // 1000
    return 'R', latency

# Check if there exist a set in list 1 that has more than or equal 2 common elements in a set of list 2
def has_mt2_common(list_set_items1, list_set_items2):
    for item_set1 in list_set_items1:
        for item_set2 in list_set_items2:
            if len(item_set1 & item_set2) > 1:
                return True
    return False
