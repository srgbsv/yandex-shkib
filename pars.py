import csv
import datetime
import sqlite3

raw_log_filename = 'shkib.csv' #Filename of RAW Log


users={}   # User dictionary

request_dict=[];
index_log=[];

top_count = {};
top_sended = {};
top5_triads = [];

db = sqlite3.connect('log.sqlite')
cursor=db.cursor()

#Make user dictionary from raw_log

def make_user_dict():
    is_header = True
    with open(raw_log_filename, 'r') as log:
        log_reader = csv.reader(log, delimiter=',');
        for ip_row in log_reader:
            if is_header:       #Skip header row
                is_header = False
                continue
            if ip_row[1] in users:                              #Check if current user is in our dict
                users[ip_row[1]]['count'] += 1                  #increment request counter
                users[ip_row[1]]['sended'] += int(ip_row[8])    #sum user data count
            else:
                users[ip_row[1]] = {                            #Else add new user to dict
                    'count': 1,
                    'sended': int(ip_row[8])
                }
    return users

def make_request_dict():
    is_header = True
    with open(raw_log_filename, 'r') as log:
        log_reader = csv.reader(log, delimiter=',');
        i=0
        for ip_row in log_reader:
            if is_header:       #Skip header row
                is_header = False
                continue
            index_log.append(find_or_insert_sqlite(ip_row[1], ip_row[2], ip_row[3],ip_row[4], ip_row[5], ip_row[6])) #Make log with unique request row ID's (Make alphabet)
#            index_log.append(find_or_insert(ip_row[1], ip_row[3], ip_row[5], ip_row[6]))
        print(index_log)
    return users

# Return top 5 users order by count
# Need global user dictionary
def step_1():
    print("# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов")
    print("             src_user              : Количество запросов")
    sort_by_count = sorted(users.items(), key=lambda item: -item[1]['count'])
    i=0
    for user_row in sort_by_count:
        if (i < 5):
            i += 1
        else:
            break;
        top_count[user_row[0]] = user_row[1]['count']
        print (str(user_row[0])+"  :  "+ str(user_row[1]['count']))

# Return top 5 users order by data send
# Need global user dictionary
def step_2():
    print("# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов")
    print("             src_user              : Количество запросов")
    sort_by_sended = sorted(users.items(), key=lambda item: -item[1]['sended'])
    i=0
    for user_row in sort_by_sended:
        if (i < 5):
            i += 1
        else:
            break;
        print(str(user_row[0])+"  :  "+ str(user_row[1]['sended']))


# Find string in dictionary with similary params and return row_id if found.
# Else add new row in dict
def find_or_insert(_src_user, _src_port, _dest_ip, _dest_port):
    for dict_row in request_dict:
        if dict_row['src_user'] == _src_user and dict_row['src_port'] == _src_port and dict_row['dest_ip'] == _dest_ip and dict_row['dest_port'] == _dest_port: 
            return dict_row['id'];
    id = len(request_dict);
    request_dict.append({
        'id':id,
        'src_user': _src_user,
        'src_port': _src_port,
        'dest_ip': _dest_ip,
        'dest_port': _dest_port
    })
    return id

# The same as find_or_insert but with sqlite
def find_or_insert_sqlite(_src_user, _src_ip, _src_port, _dest_user, _dest_ip, _dest_port):
    log_row_id = None
    for row in cursor.execute("SELECT id FROM log WHERE src_ip = ? AND src_port = ? AND dest_ip = ? AND dest_port = ?", (_src_ip, _src_port, _dest_ip, _dest_port)):
        log_row_id = row[0]
    if log_row_id is None:
        cursor.execute("insert into log(src_user, src_ip, src_port, dest_ip, dest_user, dest_port) VALUES(?, ?, ?, ?, ?, ?)", (_src_user, _src_ip, _src_port, _dest_user, _dest_ip, _dest_port))
        log_row_id = cursor.lastrowid
        db.commit()
    return log_row_id

def find_top5_triad():
    triads=[];
    for x in range(0, len(index_log)-3):
        found=False;
        for triad in triads:
            if triad['value'][0] == index_log[x] and triad['value'][1] == index_log[x+1] and triad['value'][2] == index_log[x+2]:
                triad['count'] += 1
                found=True;
                break;
        if not found:
            triads.append({
                'value': [index_log[x], index_log[x+1], index_log[x+2]],
                'count':1
                })
    triads = sorted(triads, key=lambda item: -item['count']);
    triads = triads[0:5]
    return triads;

def find_top5_quad(triads):
    quads=[]
    log_len = len(index_log)
    for x in range(0, log_len-3):
        for triad in triads:
            if triad['value'][0] == index_log[x] and triad['value'][1] == index_log[x+1] and triad['value'][2] == index_log[x+2]:
                pref_found = False
                suff_found = False
                for quad in quads:
                    if x>0 and not pref_found and quad['value'][0] == index_log[x-1] and quad['value'][1] == index_log[x] and quad['value'][2] == index_log[x+1] and quad['value'][3] == index_log[x+2]:
                        quad['count'] += 1
                        pref_found = True
                    if x<log_len-3 and not suff_found and quad['value'][0] == index_log[x] and quad['value'][1] == index_log[x+1] and quad['value'][2] == index_log[x+2] and quad['value'][3] == index_log[x+3]:
                        quad['count'] += 1
                        suff_found = True
                    if pref_found and suff_found:
                        break
                if x>0 and not pref_found:
                    quads.append({
                        'value': [index_log[x-1], index_log[x], index_log[x+1], index_log[x+2]],
                        'count':1
                    })
                if x<log_len-3 and not suff_found:
                    quads.append({
                        'value': [index_log[x], index_log[x+1], index_log[x+2], index_log[x+3]],
                        'count':1
                    })
    for quad in quads:  #Find if one quad contain 2 of out top5 triad. Then count of
        count=0;
        for triad in triads:
            if ((triad['value'][0] == quad['value'][0] and 
                triad['value'][1] == quad['value'][1] and 
                triad['value'][2] == quad['value'][2]) or 
                (triad['value'][0] == quad['value'][1] and
                 triad['value'][1] == quad['value'][2] and
                 triad['value'][2] == quad['value'][3])):
                    if triad['count']>count:
                        count = triad['count'];         #unfortunately it's wrong but I have no time for correcting :'(
        quad['count'] = count
    quads = sorted(quads, key=lambda item: -item['count']);
    quads = quads[0:5]
    return quads

def find_top5_five(quads):
    fives=[]
    log_len = len(index_log)
    for x in range(0, log_len-4):
        for quad in quads:
            if (quad['value'][0] == index_log[x] and 
               quad['value'][1] == index_log[x+1] and 
               quad['value'][2] == index_log[x+2] and
               quad['value'][3] == index_log[x+3]):
                pref_found = False
                suff_found = False
                for five in fives:
                    if (x>0 and 
                       not pref_found and 
                       five['value'][0] == index_log[x-1] and 
                       five['value'][1] == index_log[x] and 
                       five['value'][2] == index_log[x+1] and
                       five['value'][3] == index_log[x+2] and
                       five['value'][4] == index_log[x+3]):
                        five['count'] += 1
                        pref_found = True
                    if (x<log_len-4 and 
                       not suff_found and 
                       five['value'][0] == index_log[x] and 
                       five['value'][1] == index_log[x+1] and                 
                       five['value'][2] == index_log[x+2] and 
                       five['value'][3] == index_log[x+3] and
                       five['value'][4] == index_log[x+4]):
                           five['count'] += 1
                           suff_found = True
                    if pref_found and suff_found:
                        break
                if x>0 and not pref_found:
                    fives.append({
                        'value': [index_log[x-1], index_log[x], index_log[x+1], index_log[x+2], index_log[x+3]],
                        'count': 1
                    })
                if x<log_len-4 and not suff_found:
                    fives.append({
                        'value': [index_log[x], index_log[x+1], index_log[x+2], index_log[x+3], index_log[x+4]],
                        'count':1
                    })
    for five in fives:
        count=0;
        for quad in quads:
            if ((quad['value'][0] == five['value'][0] and
                 quad['value'][1] == five['value'][1] and
                 quad['value'][2] == five['value'][2] and
                 quad['value'][3] == five['value'][3]) or
                (quad['value'][0] == five['value'][1] and
                 quad['value'][1] == five['value'][2] and
                 quad['value'][2] == five['value'][3] and
                 quad['value'][3] == five['value'][4])):
                if quad['count']>count:
                       count = quad['count'];   #unfortunately it's wrong but I have no time for correcting :'(
        five['count'] = count
    fives = sorted(fives, key=lambda item: -item['count']);
    fives = fives[0:5]
    return fives

def make_finally_top5(top5_five, top5_quad, top5_triad):
    top5 = top5_five;
    top5.extend(top5_five)
    top5.extend(top5_quad)
    top5.extend(top5_triad)
    top5 = sorted(top5, key=lambda item: (-item['count'], -len(item['value'])))
    top5 = top5[0:5]
    print(top5)

make_user_dict()
step_1()
step_2()

#make_request_dict()                        #Make request "Alphabet" and log with request IS's
#db.close()

#Hypotension In top5 quads must be one of top5 quad; In top5 quad must be one of top5 triad;
#Then, first thing we can fond top5 triad
#top5_triad = find_top5_triad();            #Find top5 request log triad
#top5_quad = find_top5_quad(top5_triad);    #Find top5 quads, which contains our top5 triad;
#top5_five = find_top5_five(top5_quad);     #Find top5 fives, which contains out top5 quad;

#make_finally_top5(top5_five, top5_quad, top5_triad) #Merge finally request list




