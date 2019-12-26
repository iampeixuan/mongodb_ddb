import os
import sys
import json
import pickle
import pymongo
import datetime
import pandas as pd

# get the user input arguments
args = sys.argv[1:]
if len(args) != 2:
    print("please give exact arguments: [IP] [database_name] ")
    print("terminating program...")
    exit(1)
IP, database_name = args
if IP == 'local':
    IP = '127.0.0.1'

# test connection and work on collection
try:
    client = pymongo.MongoClient(IP, serverSelectionTimeoutMS=1)
    print(client.server_info())
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
    print("terminating program...")
    exit(1)

db = getattr(pymongo.MongoClient(IP, 27017, serverSelectionTimeoutMS=600000), database_name)
# enable sharding on the database
client.admin.command('enableSharding', database_name)
print("working on database : ", db.name)

# enable sharding on the collections on default index
collection1 = database_name + ".collection1"
client.admin.command('shardCollection', collection1, key={'_id': 1})
collection2 = database_name + ".collection2"
client.admin.command('shardCollection', collection2, key={'_id': 1})
collection3 = database_name + ".collection3"
client.admin.command('shardCollection', collection3, key={'_id': 1})
collection4 = database_name + ".collection4"
client.admin.command('shardCollection', collection4, key={'_id': 1})

short_cut = False
if short_cut:
    print("loading data directly")
    with open('c1.pkl', 'rb') as f:
        c1 = pickle.load(f)
        result = db.collection1.insert_many(c1)
        print("inserted {} documents".format(len(result.inserted_ids)))
    with open('c2.pkl', 'rb') as f:
        c2 = pickle.load(f)
        result = db.collection2.insert_many(c2)
        print("inserted {} documents".format(len(result.inserted_ids)))
    with open('c3.pkl', 'rb') as f:
        c3 = pickle.load(f)
        result = db.collection3.insert_many(c3)
        print("inserted {} documents".format(len(result.inserted_ids)))
    with open('c4.pkl', 'rb') as f:
        c4 = pickle.load(f)
        result = db.collection4.insert_many(c4)
        print("inserted {} documents".format(len(result.inserted_ids)))

else:
    print("loading csv files...")
    # load the original tables from csv files
    warehouse_df = pd.read_csv(r'./DB-project-files/data-files/warehouse.csv', header=None)
    warehouse_df.columns = ['w_id', 'w_name', 'w_street_1', 'w_street_2', 'w_city', 'w_state', 'w_zip', 'w_tax', 'w_ytd']

    district_df = pd.read_csv(r'./DB-project-files/data-files/district.csv', header=None)
    district_df.columns = ['w_id', 'd_id', 'd_name', 'd_street_1', 'd_street_2', 'd_city', 'd_state', 'd_zip', 'd_tax', 'd_ytd', 'd_next_o_id']

    customer_df = pd.read_csv(r'./DB-project-files/data-files/customer.csv', header=None)
    customer_df.columns = ['w_id', 'd_id', 'c_id', 'c_first', 'c_middle', 'c_last', 'c_street_1', 'c_street_2', 'c_city', 'c_state', 'c_zip', 'c_phone', 'c_since', 'c_credit', 'c_credit_lim', 'c_discount', 'c_balance', 'c_ytd_payment', 'c_payment_cnt', 'c_delivery_cnt', 'c_data']

    item_df = pd.read_csv(r'./DB-project-files/data-files/item.csv', header=None)
    item_df.columns = ['i_id', 'i_name', 'i_price', 'i_im_id', 'i_data']

    stock_df = pd.read_csv(r'./DB-project-files/data-files/stock.csv', header=None)
    stock_df.columns = ['w_id', 'i_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt', 's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08','s_dist_09','s_dist_10', 's_data']

    order_df = pd.read_csv(r'./DB-project-files/data-files/order.csv', header=None, low_memory=False)
    order_df.columns = ['w_id', 'd_id', 'o_id', 'c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d']
    order_df['o_carrier_id'].fillna(0, inplace=True)
    order_df['o_carrier_id'] = order_df['o_carrier_id'].astype(int)

    order_line_df = pd.read_csv(r'./DB-project-files/data-files/order-line.csv', header=None, low_memory=False)
    order_line_df.columns = ['w_id', 'd_id', 'o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']
    order_line_df['ol_delivery_d'].fillna(str(datetime.datetime(2000, 1, 1, 0, 0, 0)), inplace=True)
    num_ol_row, num_ol_col = order_line_df.shape

    """ create collection 1 """
    print("creating collection 1")
    t_start = datetime.datetime.now()

    # collection 1: all the variables of warehouse and district, nested (1 warehouse -> 10 districts)
    # convert pd dataframe to dict
    warehouse_var_dict_list = warehouse_df[['w_id', 'w_ytd']].to_dict('records')

    for w in range(1, 11):
        # group district by w_id, convert pd dataframe to dictionary
        district_var_dict_list = district_df.loc[district_df['w_id'] == w][['d_id', 'd_ytd', 'd_next_o_id']].to_dict('records')
        # add 'districts' entry to warehouse dictionary
        warehouse_var_dict_list[w-1]['districts'] = district_var_dict_list

    # batch insert to mongodb
    result = db.collection1.insert_many(warehouse_var_dict_list)
    print("inserted {} documents".format(len(result.inserted_ids)))

    t_end = datetime.datetime.now()
    print("time (seconds) for inserting collection 1: ", (t_end - t_start).seconds)

    with open('c1.pkl', 'wb') as f:
        pickle.dump(warehouse_var_dict_list, f)

    """ create collection 2 """
    print("creating collection 2")
    t_start = datetime.datetime.now()

    # collection 2: customer table join with static fields from warehouse and district, and item_ordered
    # select the static fields from warehouse
    warehouse_data = warehouse_df[['w_id', 'w_name', 'w_street_1', 'w_street_2', 'w_city', 'w_state', 'w_zip', 'w_tax']]
    # select the static fields from district
    district_data = district_df[['w_id', 'd_id', 'd_name', 'd_street_1', 'd_street_2', 'd_city', 'd_state', 'd_zip', 'd_tax']]
    # merge customer warehouse and district together (static fields only)
    warehouse_district_data = warehouse_data.merge(district_data, on=['w_id'], how='right')
    warehouse_district_customer_data = warehouse_district_data.merge(customer_df, on=['w_id', 'd_id'], how='right')
    # convert dataframe to dict (this is the one to upload)
    customer_dict_list = warehouse_district_customer_data.to_dict('records')

    # add c_id column into order-line
    order_data = order_df[['w_id', 'd_id', 'o_id', 'c_id']]
    order_line_data = order_line_df[['w_id', 'd_id', 'o_id', 'ol_i_id']]
    order_order_line_data = order_line_data.merge(order_data, on=['w_id', 'd_id', 'o_id'], how='left')

    # to group the items ordered by each customer, separated by o_id
    customer_orders = {}  # index: w_d_c_id, value: array of json [{o_id: x, items: [a, b, c]}]
    for index, row in order_order_line_data.iterrows():
        w_d_c_id = "{},{},{}".format(row['w_id'], row['d_id'], row['c_id'])
        o_id = int(row['o_id'])
        i_id = int(row['ol_i_id'])
        if w_d_c_id in customer_orders.keys():
            # retrieve data
            order_seen = False
            order_json_array = customer_orders[w_d_c_id]
            for i in range(len(order_json_array)):
                # check if this order is seen before
                if o_id == order_json_array[i]["o_id"]:
                    items = order_json_array[i]["items"]
                    items.append(i_id)
                    customer_orders[w_d_c_id][i]["items"] = items
                    order_seen = True
            if not order_seen:
                # add this new order in
                temp_dict = {
                    "o_id": o_id,  # same customer but different order
                    "items": [i_id]
                }
                order_json_array.append(temp_dict)
                customer_orders[w_d_c_id] = order_json_array
        else:
            # create new entry with a dictionary
            temp_dict = {
                "o_id": o_id,
                "items": [i_id]  # to append here if same order
            }
            customer_orders[w_d_c_id] = [temp_dict]

        if index % 1000000 == 0:
            print("{}/{} order-lines processed in {}s".format(index, num_ol_row, (datetime.datetime.now() - t_start).seconds))

    # add field "items_ordered" into each customer
    for index in range(len(customer_dict_list)):
        temp_dict = customer_dict_list[index]
        w_d_c_id = "{},{},{}".format(temp_dict['w_id'], temp_dict['d_id'], temp_dict['c_id'])
        if w_d_c_id in customer_orders.keys():
            customer_dict_list[index]["items_ordered"] = customer_orders[w_d_c_id]
        else:
            customer_dict_list[index]["items_ordered"] = []

    # batch insert to mongodb
    result = db.collection2.insert_many(customer_dict_list)
    print("inserted {} documents".format(len(result.inserted_ids)))

    t_end = datetime.datetime.now()
    print("time (seconds) for inserting collection 2: ", (t_end - t_start).seconds)

    with open('c2.pkl', 'wb') as f:
        pickle.dump(customer_dict_list, f)

    """ create collection 3 """
    print("creating collection 3")
    t_start = datetime.datetime.now()

    # combine order and order-line
    # group order_lines into array of json list based on w_id, d_id and o_id
    order_line_dict = {}
    for index, row in order_line_df.iterrows():
        # convert order-line to a dict, remove some redundant fields
        temp_dict = row.to_dict()
        del temp_dict["w_id"]
        del temp_dict["d_id"]
        del temp_dict["o_id"]

        w_d_o_id = "{},{},{}".format(row['w_id'], row['d_id'], row['o_id'])
        if w_d_o_id in order_line_dict.keys():
            # append dict into list
            array = order_line_dict[w_d_o_id]
            array.append(temp_dict)
            order_line_dict[w_d_o_id] = array
        else:
            # create new array of dict
            order_line_dict[w_d_o_id] = [temp_dict]

        if index % 1000000 == 0:
            print("{}/{} order-lines processed in {}s".format(index, num_ol_row, (datetime.datetime.now() - t_start).seconds))

    # convert df to python dictionary
    order_dict_list = order_df.to_dict('records')

    # add "order_lines" entry to order dict
    for index in range(len(order_dict_list)):
        order = order_dict_list[index]
        w_d_o_id = "{},{},{}".format(order['w_id'], order['d_id'], order['o_id'])
        order_dict_list[index]['order_lines'] = order_line_dict[w_d_o_id]

    # batch insert to mongodb
    result = db.collection3.insert_many(order_dict_list)
    print("inserted {} documents".format(len(result.inserted_ids)))

    t_end = datetime.datetime.now()
    print("time (seconds) for inserting collection 3: ", (t_end - t_start).seconds)

    with open('c3.pkl', 'wb') as f:
        pickle.dump(order_dict_list, f)

    """ create collection 4 """
    print("creating collection 4")
    t_start = datetime.datetime.now()

    # join stock and item together
    item_stock_df = item_df.merge(stock_df, on=['i_id'], how='left')
    # convert pd dataframe to python dictionary
    item_stock_dict_list = item_stock_df.to_dict('records')

    result = db.collection4.insert_many(item_stock_dict_list)
    print("inserted {} documents".format(len(result.inserted_ids)))

    t_end = datetime.datetime.now()
    print("time (seconds) for inserting collection 4: ", (t_end - t_start).seconds)

    with open('c4.pkl', 'wb') as f:
        pickle.dump(item_stock_dict_list, f)

# create index on collections
db.collection1.create_index([("w_id", 1)])
db.collection2.create_index([("c_balance", -1)])
db.collection2.create_index([("w_id", 1), ("d_id", 1), ("c_id", 1)])
db.collection3.create_index([("w_id", 1), ("d_id", 1), ("c_id", 1), ("o_id", -1)])
db.collection3.create_index([("w_id", 1), ("d_id", 1), ("o_id", 1)])
db.collection4.create_index([("i_id", 1), ("w_id", 1), ("s_quantity", 1)])
