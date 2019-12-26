import pymongo
import datetime


def xact_I(database, xact):
    t1 = datetime.datetime.now()
    print("========== POPULAR-ITEM TRANSACTION ==========")

    # get input
    line = xact.rstrip().split(',')
    w_id = int(line[1])
    d_id = int(line[2])
    l = int(line[3])
    print("In warehouse %d district %d, search for the most popular item each in the last %d order" % (w_id, d_id, l))

    next_order_no = database.collection1.find_one({"w_id": w_id}, {"districts"})["districts"][d_id - 1]["d_next_o_id"]
    # print(next_order_no)

    # Gather all items from the last l orders
    popular_item_ids = set()
    itemId_name = {}
    projection = {"order_lines", "o_id", "o_entry_d"}
    orders = database.collection3.find({"w_id": w_id, "d_id": d_id, "o_id": {"$lt": next_order_no, "$gte": next_order_no - l}}, projection)
    # orders_it = database.collection3.find({"w_id": 1, "d_id": 1, "o_id": {"$lt": 3001, "$gte": 3001 - 1}})
    # print(orders_it.count())

    orders_items_map = {}
    total_orders_count = 0
    for order in orders:
        total_orders_count += 1
        # order["order_lines"].sort(key=lambda ol: ol["ol_quantity"], reverse=True)
        highest_quantity = 0
        items = set()
        # Find the highest quantity
        for order_line in order["order_lines"]:
            items.add(order_line["ol_i_id"])
            if order_line["ol_quantity"] > highest_quantity:
                highest_quantity = order_line["ol_quantity"]
        orders_items_map[order["o_id"]] = items
        # print(highest_quantity)

        # Gather all the popular items in the order and query items' name
        for order_line in order["order_lines"]:
            if order_line["ol_quantity"] == highest_quantity:
                popular_item_ids.add(order_line["ol_i_id"])
                if itemId_name.get(order_line["ol_i_id"]) == None:
                    # TODO: Batch query of item name instead
                    i_name = database.collection4.find_one({"w_id": w_id, "i_id": order_line["ol_i_id"]})["i_name"]
                    # print(i_name)
                    itemId_name[order_line["ol_i_id"]] = i_name

        # Query customer name for the order
        cus_projection = {"c_first", "c_middle", "c_last"}
        customer = database.collection2.find_one({"w_id": w_id, "d_id": d_id, "items_ordered": {"$elemMatch": {"o_id": order["o_id"]}}}, cus_projection)
        print("Popular item(s) in order %d (%s) by customer %s %s %s:" % (order["o_id"], order["o_entry_d"], customer["c_first"], customer["c_middle"], customer["c_last"]))
        # Print all the popular item name and its quantity
        for item_ID in popular_item_ids:
            print("%s %s" % (itemId_name[item_ID], highest_quantity))

    # For each pop item, find num of orders that has that item / total orders
    for item_id in popular_item_ids:
        count = 0
        for setItems in orders_items_map.values():
            if item_id in setItems:
                count += 1
        percentage = (count / total_orders_count) * 100
        print("Popular item %s has %.2f%% coverage in all orders" % (itemId_name[item_id], percentage))

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'I', latency
