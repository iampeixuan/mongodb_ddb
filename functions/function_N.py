import pymongo
import datetime


def xact_N(database, xact):
    t1 = datetime.datetime.now()
    print("========== NEW ORDER TRANSACTION ==========")
    lines = [line.rstrip().split(',') for line in xact]

    # get the input
    c_id = int(lines[0][1])
    w_id = int(lines[0][2])
    d_id = int(lines[0][3])
    total_items = int(lines[0][4])
    if int(d_id) != 10:
        txt_d_id = '0' + str(d_id)
    else:
        txt_d_id = str(d_id)

    # get the customer info and tax rates
    result = database.collection2.find({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"_id": 0, "c_last": 1, "c_credit":1, "c_discount": 1, "d_tax": 1, "w_tax": 1})
    for row in result:
        c_last = row["c_last"]
        c_credit = row["c_credit"]
        c_discount = row["c_discount"]
        w_tax = row["d_tax"]
        d_tax = row["w_tax"]

    # read and update d_next_o_id
    #result = database.collection1.find({"w_id": w_id}, {"districts": {"$elemMatch": {"d_id": d_id}}})
    result = database.collection1.find({"w_id": w_id}, {"w_id": 1, "districts": {"$slice": [d_id - 1, 1]}, "_id": 0})
    for row in result:
        N = row["districts"][0]['d_next_o_id']
    result = database.collection1.update({"$and": [{"w_id": w_id}, {"districts.d_id": d_id}]}, {"$set": {"districts.$.d_next_o_id": N + 1}})

    # check if all local warehouse
    warehouse_id = set([line[1] for line in lines[1:]])
    if (len(warehouse_id) == 1) and (w_id in warehouse_id):
        all_local = 1
    else:
        all_local = 0

    # check stock and create order + order-line for each item
    current_ts = str(datetime.datetime.now()) #.strftime("%Y-%m-%d %H:%M:%S")
    order_dict = {
        "w_id": w_id,
        "d_id": d_id,
        "o_id": N,
        "c_id": c_id,
        "o_carrier_id": 0,
        "o_ol_cnt": total_items,
        "o_all_local": all_local,
        "o_entry_d": current_ts,
        "order_lines": []  # place holder
    }

    total_amount = 0
    item_counter = 0
    item_ids = []
    item_info = []
    # loop through every item
    for line in lines[1:]:
        # get the input
        item_id = int(line[0])
        sup_warehouse = int(line[1])
        quant = int(line[2])
        item_counter += 1

        # read from stock + item
        result = database.collection4.find({"i_id": item_id, "w_id": w_id}, {'s_dist_' + txt_d_id: 1, 's_quantity': 1, 's_ytd': 1, 's_order_cnt': 1, 's_remote_cnt': 1, 'i_price': 1, 'i_name': 1, "_id": 0})
        for row in result:
            s_dist = row['s_dist_' + txt_d_id]
            s_quant = row['s_quantity']
            s_ytd = row['s_ytd']
            s_order_cnt = row['s_order_cnt']
            s_remote_cnt = row['s_remote_cnt']
            price = row['i_price']
            name = row['i_name']

        # adjust the stock quantity
        adjusted_quant = s_quant - quant
        if adjusted_quant < 10:
            adjusted_quant += 100
        if sup_warehouse != w_id:
            s_remote_cnt += 1

        # update stock + item
        result = database.collection4.update({"w_id": sup_warehouse, "i_id": item_id}, {"$set": {"s_quantity": adjusted_quant, "s_ytd": s_ytd + quant, "s_order_cnt": s_order_cnt + 1, "s_remote_cnt": s_remote_cnt}})

        # calculate item amount and update total amount
        item_amount = price * quant
        total_amount += item_amount
        none_ts = str(datetime.datetime(2000, 1, 1, 0, 0, 0))

        # add this order line to order_dict
        order_line = {
            "w_id": w_id,
            "d_id": d_id,
            "o_id": N,
            "ol_number": item_counter,
            "ol_i_id": item_id,
            "ol_delivery_d": none_ts,
            "ol_amount": item_amount,
            "ol_supply_w_id": sup_warehouse,
            "ol_quantity": quant,
            "ol_dist_info": s_dist
        }
        order_dict["order_lines"].append(order_line)

        # record the items ordered
        item_ids.append(item_id)
        # record info for output
        item_info.append([item_id, name, sup_warehouse, quant, item_amount, adjusted_quant])

    # insert into order + order_line, all at once
    database.collection3.insert_one(order_dict)

    # update customer_items
    items_ordered = {
        "o_id": N,
        "items": item_ids
    }
    result = database.collection2.update({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"$push": {"items_ordered": items_ordered}})

    # calcuate total amount after tax and discount
    total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)

    # output
    print("Customer last name: {}, credit: {}, discount: {}".format(c_last, c_credit, round(c_discount, 3)))
    print("Warehouse tax: {}, District rax: {}".format(round(w_tax, 3), round(d_tax, 3)))
    print("Order number: {}, Entry time: {}".format(N, current_ts))
    print("Number of items: {}, Total amount: {}".format(total_items, round(total_amount, 3)))
    print("[Item ID, Item Name, Supplier Warehouse, Quantity, Item Amount, Stock Quantity]")
    for i in item_info:
        print(i)

    print()

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'N', latency
