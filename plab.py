import psycopg2
import psycopg2.extras
import configparser
import datetime
import json
import browser_cookie3
import urllib3
import requests
import time
import re
import os
import os.path


def get_now():
    return datetime.datetime.now().replace(microsecond=0)


def get_plab_urls():
    filename = '.\\urls_to_get.txt'
    urls = []
    with open(filename, 'r') as f:
        data = f.readlines()
    for dat in data:
        row = dat.strip()
        if row.startswith('#') or row.startswith(';') or row == '':
            continue
        else:
            urls.append(row)
    print('Found {} urls to get from plab'.format(len(urls)))
    return urls


def get_new_run_id():
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_get_run_id = '''SELECT max(run_id) from plab2.runs'''
            cur.execute(sql_get_run_id)
            res = cur.fetchone()
            if res[0] is None:
                run_id = 1
            else:
                run_id = res[0] + 1
    return run_id


def start_new_run(run_id):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_start_run = '''INSERT INTO plab2.runs (run_id, status, start_dttm, end_dttm)
                        VALUES(%s, %s, %s, %s);'''
            cur.execute(sql_start_run, (run_id, None, datetime.datetime.now(), None))
            conn.commit()


def get_from_file(urls):
    '''
    Получение данных с plab'а
    :param urls: массив url, из которых собираем json'ы
    :return: словарь. ключ - ссылка, значение - jsonы этой ссылки
    '''

    result = {}

    for url in urls:
        with open('.\\data\\response.json', 'r', encoding='utf8') as f:
            data = f.read()
            data_json = json.loads(data)
            result[url] = data_json
    return result

def save_recieved_plab_answer(run_id, resp):
    '''
    Saves results of request.get() to file in .\data\{run_id}\ in 2 files:
    meta.txt - some attributes of the answer
    text.txt - data from resp.text (string)
    cont.txt - from resp.content (bytes)
    :param resp: - result of "requests.get(...)"
    :return:
    '''

    if not os.path.exists('.\\data\\'):
        os.mkdir('.\\data\\')
    if not os.path.exists('.\\data\\run_id_{}\\'.format(run_id)):
        os.mkdir('.\\data\\run_id_{}\\'.format(run_id))
    with open('.\\data\\run_id_{}\\meta.txt'.format(run_id), 'w', newline='') as m:
        meta_data = []
        meta_data.append('{}:\n{}\n\n'.format('request.url', resp.request.url))
        meta_data.append('{}:\n{}\n\n'.format('url', resp.url))
        meta_data.append('{}:\n{}\n\n'.format('status_code', resp.status_code))
        meta_data.append('{}:\n{}\n\n'.format('ok', resp.ok))
        meta_data.append('{}:\n{}\n\n'.format('reason', resp.reason))
        meta_data.append('{}:\n{}\n\n'.format('encoding', resp.encoding))
        meta_data.append('{}:\n{}\n\n'.format('headers', str(resp.headers)))
        meta_data.append('{}:\n{}\n\n'.format('resp.is_permanent_redirect', resp.is_permanent_redirect))
        meta_data.append('{}:\n{}\n\n'.format('resp.is_redirect', resp.is_redirect))
        meta_data.append('{}:\n{}\n\n'.format('links', resp.links))
        m.writelines(meta_data)
    with open('.\\data\\run_id_{}\\text.txt'.format(run_id), 'w', encoding='utf8') as t:
        t.write(resp.text)

    with open('.\\data\\run_id_{}\\cont.txt'.format(run_id), 'wb') as t:
        t.write(resp.content)

        pass


def get_from_plabforum(run_id, urls):
    '''
    Получение данных с plab'а
    :param urls: массив url, из которых собираем json'ы
    :return: словарь. ключ - ссылка, значение - jsonы этой ссылки
    '''

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    cookies = browser_cookie3.chrome(domain_name='.pornolab.net')
    headers = {
        'referer': 'https://pornolab.net/forum/index.php',
        'host': "pornolab.net"
    }

    result = {}
    has_errors = 0
    status = (has_errors, '')

    i = 0
    for url in urls:
        i += 1
        print('Processing URL ({} of {}): {}'.format(i, len(urls), url))
        try:
            r = requests.get(url, verify=False, headers=headers, cookies=cookies, timeout=15)
        except Exception as e:
            has_errors += 1
            status = (has_errors, 'Connection error: {}'.format(str(e)))
            save_recieved_plab_answer(run_id, r)
            return status, result

        try:
            j = json.loads(r.text)
        except Exception as e:
            has_errors += 1
            status = (has_errors, 'JSON parsing_error: {}'.format(str(e)))
            save_recieved_plab_answer(run_id, r)
            return status, result

        print('     Recieved {} topics'.format(len(j)))
        print()
        result[url] = j

    return status, result


def get_jsons_from_plab(run_id, urls):
    # result = get_from_file(urls)
    status, result = get_from_plabforum(run_id, urls)
    if status[0] != 0:
        return status, result

    for url in result.keys():
        # print('    Recieved {} topics'.format(len(result[url])))
        for topic in result[url]:
            title = str(topic['TOPIC_TITLE'])
            title = title.replace('<wbr>', '').replace('<b>', '').replace('</b>', '')
            title = title.replace('&quot;', '"').replace('&amp;', '&')
            unicode_symbols = re.findall(r'&#\d\d\d;', title)
            for unicode_symbol in unicode_symbols:
                title = title.replace(unicode_symbol, chr(int(unicode_symbol[2:-1])))
            # title = title.replace('&#039;', '\'')
            # title = title.replace('&#246;', chr(246))
            # title = title.replace('&#228;', chr(228))

            topic['TOPIC_TITLE'] = title
            topic['POSTER_NAME'] = str(topic['POSTER_NAME']).replace('<wbr>', '')
            topic['FORUM_NAME'] = str(topic['FORUM_NAME']).replace('&amp;', '&').replace('&#039;', '\'')
            topic['TOR_SIZE'] = str(topic['TOR_SIZE']).replace('&nbsp;', ' ')

    return status, result


def get_urls_data():
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    result = {}
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql_get_data = '''SELECT url_id, url FROM plab2.urls'''
            cur.execute(sql_get_data)
            url_data = cur.fetchall()
            for row in url_data:
                url_id = row['url_id']
                url = row['url']
                result[url] = url_id
    return result


def save_new_url(url):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    url_id = None
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql_save_url = '''INSERT INTO plab2.urls (url) VALUES (%s) RETURNING (url_id)'''
            cur.execute(sql_save_url, (url, ))
            url_id = cur.fetchone()['url_id']
    return url_id


def update_forums(data):
    forums_data = []
    for forum_id in data.keys():
        forums_data.append((forum_id, data[forum_id]))

    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_update_forums = '''INSERT INTO plab2.forums (forum_id, forum_name) VALUES (%s, %s) ON CONFLICT (forum_id) DO NOTHING'''
            cur.executemany(sql_update_forums, forums_data)
        conn.commit()


def update_posters(data):
    posters_data = []
    for poster_id in data.keys():
        posters_data.append((poster_id, data[poster_id]))

    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_update_posters = '''INSERT INTO plab2.posters (poster_id, poster_name) VALUES (%s, %s) ON CONFLICT (poster_id) DO NOTHING'''
            cur.executemany(sql_update_posters, posters_data)
        conn.commit()


def save_url_topics(data):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_update_posters = '''INSERT INTO plab2.url_topics (url_id, topic_id, run_id) VALUES (%s, %s, %s)'''
            cur.executemany(sql_update_posters, data)
        conn.commit()


# def get_topic_info(topic_id):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_topic_info = '''SELECT * from plab2.topics WHERE topic_id = %s'''
#             cur.execute(sql_topic_info, (topic_id,))
#             data = cur.fetchall()
#             return data


def get_all_topics_info():
    result = {}
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql_topic_info = '''SELECT * from plab2.topics'''
            cur.execute(sql_topic_info)
            data = cur.fetchall()
            for dat in data:
                result[dat['topic_id']] = dat
            return result


# def insert_new_topic(run_id, topic):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_topic_insert = '''INSERT INTO plab2.topics
#             (topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text, tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm, user_author, tor_frozen, seed_never_seen)
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#             '''
#             values_topic = (topic['TOPIC_ID'], run_id, run_id, topic['TOPIC_TITLE'], topic['TOPIC_TIME'], topic['POSTER_ID'],
#                       topic['FORUM_ID'], topic['TOR_STATUS_TEXT'], topic['TOR_SIZE'], topic['TOR_SIZE_INT'],
#                       topic['TOR_PRIVATE'], topic['INFO_HASH'], topic['ADDED_TIME'], topic['ADDED_DATE'],
#                       topic['ADDED_INT'], datetime.datetime.fromtimestamp(topic['ADDED_INT']),
#                       topic['USER_AUTHOR'], topic['TOR_FROZEN'], topic['SEED_NEVER_SEEN'])
#             cur.execute(sql_topic_insert, values_topic)
#
#             conn.commit()


# def insert_seeding_info(run_id, topic):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_seeding_insert = '''INSERT INTO plab2.seeding_info
# (topic_id, run_id, seeds, leechs, unique_seeds, seeder_last_seen, seeder_last_seen_dttm, not_seen_days, user_seed_this, completed, keepers_cnt)
# VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
# '''
#             not_seen_days = topic['NOT_SEEN_DAYS']
#             if not_seen_days == '':
#                 not_seen_days = 0
#             values_seeding_info = (topic['TOPIC_ID'], run_id, topic['SEEDS'], topic['LEECHS'], topic['UNIQUE_SEEDS'],
#                                    topic['SEEDER_LAST_SEEN'], datetime.datetime.fromtimestamp(topic['SEEDER_LAST_SEEN']),
#                                    not_seen_days, topic['USER_SEED_THIS'],
#                                    topic['COMPLETED'], topic['KEEPERS_CNT'])
#             cur.execute(sql_seeding_insert, values_seeding_info)
#
#         conn.commit()


# def update_topic_run_id(topic_id, run_id):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_update_run_id = '''UPDATE plab2.topics SET updated_by_run_id = %s WHERE topic_id = %s'''
#             cur.execute(sql_update_run_id, (run_id, topic_id))
#         conn.commit()


# def move_topic_to_history(topic_id):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_move_topic = '''INSERT INTO plab2.topics_hist
# (topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text,
# tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm,
# user_author, tor_frozen, seed_never_seen)
# SELECT topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text, tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm, user_author, tor_frozen, seed_never_seen
# FROM plab2.topics WHERE topic_id = %s'''
#             cur.execute(sql_move_topic, (topic_id, ))
#         conn.commit()


# def update_topic(run_id, topic):
#     cfg = configparser.ConfigParser()
#     cfg.read('config.ini')
#     with psycopg2.connect(host=cfg['DEFAULT']['host'],
#                           port=int(cfg['DEFAULT']['port']),
#                           database=cfg['DEFAULT']['database'],
#                           user=cfg['DEFAULT']['user'],
#                           password=cfg['DEFAULT']['password']) as conn:
#         with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#             sql_update_topic = '''UPDATE plab2.topics SET
#             created_by_run_id=%s, updated_by_run_id=%s, topic_title=%s, topic_time=%s, poster_id=%s,
#             forum_id=%s, tor_status_text=%s, tor_size=%s, tor_size_int=%s, tor_private=%s, info_hash=%s,
#             added_time=%s, added_date=%s, added_int=%s, added_dttm=%s, user_author=%s,
#             tor_frozen=%s, seed_never_seen=%s
#             WHERE topic_id=%s;'''
#
#             values_update_topic = (run_id, run_id, topic['TOPIC_TITLE'], topic['TOPIC_TIME'], topic['POSTER_ID'],
#                       topic['FORUM_ID'], topic['TOR_STATUS_TEXT'], topic['TOR_SIZE'], topic['TOR_SIZE_INT'],
#                       topic['TOR_PRIVATE'], topic['INFO_HASH'], topic['ADDED_TIME'], topic['ADDED_DATE'],
#                       topic['ADDED_INT'], datetime.datetime.fromtimestamp(topic['ADDED_INT']),
#                       topic['USER_AUTHOR'], topic['TOR_FROZEN'], topic['SEED_NEVER_SEEN'], topic['TOPIC_ID'])
#             cur.execute(sql_update_topic, values_update_topic)
#         conn.commit()


def save_new_topics(run_id, topic_ids, plab_data):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_topic_insert = '''INSERT INTO plab2.topics
                (topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text, tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm, user_author, tor_frozen, seed_never_seen)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                '''
            sql_seeding_insert = '''INSERT INTO plab2.seeding_info
            (topic_id, run_id, seeds, leechs, unique_seeds, seeder_last_seen, seeder_last_seen_dttm, not_seen_days, user_seed_this, completed, keepers_cnt)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            '''
            values_topic_all = []
            values_seeding_info_all = []
            for topic_id in topic_ids:
                values_topic = (
                plab_data[topic_id]['TOPIC_ID'], run_id, run_id, plab_data[topic_id]['TOPIC_TITLE'], plab_data[topic_id]['TOPIC_TIME'], plab_data[topic_id]['POSTER_ID'],
                plab_data[topic_id]['FORUM_ID'], plab_data[topic_id]['TOR_STATUS_TEXT'], plab_data[topic_id]['TOR_SIZE'], plab_data[topic_id]['TOR_SIZE_INT'],
                plab_data[topic_id]['TOR_PRIVATE'], plab_data[topic_id]['INFO_HASH'], plab_data[topic_id]['ADDED_TIME'], plab_data[topic_id]['ADDED_DATE'],
                plab_data[topic_id]['ADDED_INT'], datetime.datetime.fromtimestamp(plab_data[topic_id]['ADDED_INT']),
                plab_data[topic_id]['USER_AUTHOR'], plab_data[topic_id]['TOR_FROZEN'], plab_data[topic_id]['SEED_NEVER_SEEN'])
                values_topic_all.append(values_topic)

                not_seen_days = plab_data[topic_id]['NOT_SEEN_DAYS']
                if not_seen_days == '':
                    not_seen_days = 0
                values_seeding_info = (
                plab_data[topic_id]['TOPIC_ID'], run_id, plab_data[topic_id]['SEEDS'], plab_data[topic_id]['LEECHS'], plab_data[topic_id]['UNIQUE_SEEDS'],
                plab_data[topic_id]['SEEDER_LAST_SEEN'], datetime.datetime.fromtimestamp(plab_data[topic_id]['SEEDER_LAST_SEEN']),
                not_seen_days, plab_data[topic_id]['USER_SEED_THIS'],
                plab_data[topic_id]['COMPLETED'], plab_data[topic_id]['KEEPERS_CNT'])
                values_seeding_info_all.append(values_seeding_info)

            cur.executemany(sql_topic_insert, values_topic_all)
            cur.executemany(sql_seeding_insert, values_seeding_info_all)

            conn.commit()


def save_changed_topics(run_id, topic_ids, plab_data):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_move_topic = '''INSERT INTO plab2.topics_hist
            (topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text, 
            tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm, 
            user_author, tor_frozen, seed_never_seen)
            SELECT topic_id, created_by_run_id, updated_by_run_id, topic_title, topic_time, poster_id, forum_id, tor_status_text, tor_size, tor_size_int, tor_private, info_hash, added_time, added_date, added_int, added_dttm, user_author, tor_frozen, seed_never_seen
            FROM plab2.topics WHERE topic_id = %s'''
            values_to_move_all = []
            for topic_id in topic_ids:
                values_to_move_all.append((topic_id, ))
            cur.executemany(sql_move_topic, values_to_move_all)

            sql_update_topic = '''UPDATE plab2.topics SET 
                        created_by_run_id=%s, updated_by_run_id=%s, topic_title=%s, topic_time=%s, poster_id=%s, 
                        forum_id=%s, tor_status_text=%s, tor_size=%s, tor_size_int=%s, tor_private=%s, info_hash=%s, 
                        added_time=%s, added_date=%s, added_int=%s, added_dttm=%s, user_author=%s, 
                        tor_frozen=%s, seed_never_seen=%s
                        WHERE topic_id=%s;'''
            values_update_topic_all = []
            for topic_id in topic_ids:
                values_update_topic = (run_id, run_id,
                                       plab_data[topic_id]['TOPIC_TITLE'], plab_data[topic_id]['TOPIC_TIME'],
                                       plab_data[topic_id]['POSTER_ID'], plab_data[topic_id]['FORUM_ID'],
                                       plab_data[topic_id]['TOR_STATUS_TEXT'], plab_data[topic_id]['TOR_SIZE'],
                                       plab_data[topic_id]['TOR_SIZE_INT'], plab_data[topic_id]['TOR_PRIVATE'],
                                       plab_data[topic_id]['INFO_HASH'], plab_data[topic_id]['ADDED_TIME'],
                                       plab_data[topic_id]['ADDED_DATE'], plab_data[topic_id]['ADDED_INT'],
                                       datetime.datetime.fromtimestamp(plab_data[topic_id]['ADDED_INT']),
                                       plab_data[topic_id]['USER_AUTHOR'], plab_data[topic_id]['TOR_FROZEN'],
                                       plab_data[topic_id]['SEED_NEVER_SEEN'], plab_data[topic_id]['TOPIC_ID'])
                values_update_topic_all.append(values_update_topic)
            cur.executemany(sql_update_topic, values_update_topic_all)

            sql_seeding_insert = '''INSERT INTO plab2.seeding_info
                                    (topic_id, run_id, seeds, leechs, unique_seeds, seeder_last_seen, seeder_last_seen_dttm, not_seen_days, user_seed_this, completed, keepers_cnt)
                                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    '''
            values_seeding_info_all = []
            for topic_id in topic_ids:
                not_seen_days = plab_data[topic_id]['NOT_SEEN_DAYS']
                if not_seen_days == '':
                    not_seen_days = 0
                values_seeding_info = (
                    plab_data[topic_id]['TOPIC_ID'], run_id, plab_data[topic_id]['SEEDS'],
                    plab_data[topic_id]['LEECHS'], plab_data[topic_id]['UNIQUE_SEEDS'],
                    plab_data[topic_id]['SEEDER_LAST_SEEN'],
                    datetime.datetime.fromtimestamp(plab_data[topic_id]['SEEDER_LAST_SEEN']),
                    not_seen_days, plab_data[topic_id]['USER_SEED_THIS'],
                    plab_data[topic_id]['COMPLETED'], plab_data[topic_id]['KEEPERS_CNT'])
                values_seeding_info_all.append(values_seeding_info)
            cur.executemany(sql_seeding_insert, values_seeding_info_all)
            conn.commit()


def save_unchanged_topics(run_id, topic_ids, plab_data):
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_update_run_id = '''UPDATE plab2.topics SET updated_by_run_id = %s WHERE topic_id = %s'''
            values_update_run_ids_all = []
            for topic_id in topic_ids:
                values_update_run_ids_all.append((run_id, topic_id))
            cur.executemany(sql_update_run_id, values_update_run_ids_all)
            conn.commit()

            sql_seeding_insert = '''INSERT INTO plab2.seeding_info
                        (topic_id, run_id, seeds, leechs, unique_seeds, seeder_last_seen, seeder_last_seen_dttm, not_seen_days, user_seed_this, completed, keepers_cnt)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        '''
            values_seeding_info_all = []
            for topic_id in topic_ids:
                not_seen_days = plab_data[topic_id]['NOT_SEEN_DAYS']
                if not_seen_days == '':
                    not_seen_days = 0
                values_seeding_info = (
                    plab_data[topic_id]['TOPIC_ID'], run_id, plab_data[topic_id]['SEEDS'],
                    plab_data[topic_id]['LEECHS'], plab_data[topic_id]['UNIQUE_SEEDS'],
                    plab_data[topic_id]['SEEDER_LAST_SEEN'],
                    datetime.datetime.fromtimestamp(plab_data[topic_id]['SEEDER_LAST_SEEN']),
                    not_seen_days, plab_data[topic_id]['USER_SEED_THIS'],
                    plab_data[topic_id]['COMPLETED'], plab_data[topic_id]['KEEPERS_CNT'])
                values_seeding_info_all.append(values_seeding_info)
            cur.executemany(sql_seeding_insert, values_seeding_info_all)
            conn.commit()


def save_plab_data(run_id, data_from_plab, urls_data):
    print('SAVING DATA TO DATABASE')

    for url in data_from_plab.keys():
        if url not in urls_data.keys():
            new_url_id = save_new_url(url)
            urls_data[url] = new_url_id

    print('{}: starting actualization of forums and posters'.format(get_now()))
    forums_data_tmp = {}
    posters_data_tmp = {}
    for url in data_from_plab.keys():
        for topic in data_from_plab[url]:
            forums_data_tmp[topic['FORUM_ID']] = topic['FORUM_NAME']
            posters_data_tmp[topic['POSTER_ID']] = topic['POSTER_NAME']
    update_forums(forums_data_tmp)
    update_posters(posters_data_tmp)
    print('{}: finishing actualization of forums and posters'.format(get_now()))

    url_topics_data_tmp = []

    i = 0
    for url in data_from_plab.keys():
        i += 1
        print('{}: processing url ({} of {}): {}'.format(get_now(), i, len(data_from_plab.keys()), url))
        url_id = urls_data[url]
        all_topics_info = get_all_topics_info()  # TODO В цикле, так как после обработки 1 ссылки БД уже изменилась.
        new_topics = set()
        changed_topics = set()
        unchanged_topics = set()
        for topic in data_from_plab[url]:
            topic_id = topic['TOPIC_ID']
            url_topics_data_tmp.append((url_id, topic_id, run_id))

            if topic_id not in all_topics_info.keys():
                new_topics.add(topic_id)
            else:
                topic_title_changed = all_topics_info[topic_id]['topic_title'] != topic['TOPIC_TITLE']
                topic_time_changed = all_topics_info[topic_id]['topic_time'] != topic['TOPIC_TIME']
                poster_id_changed = all_topics_info[topic_id]['poster_id'] != topic['POSTER_ID']
                forum_id_changed = all_topics_info[topic_id]['forum_id'] != topic['FORUM_ID']
                tor_status_text_changed = all_topics_info[topic_id]['tor_status_text'] != topic['TOR_STATUS_TEXT']
                tor_size_int_changed = all_topics_info[topic_id]['tor_size_int'] != topic['TOR_SIZE_INT']
                tor_private_changed = all_topics_info[topic_id]['tor_private'] != topic['TOR_PRIVATE']
                info_hash_changed = all_topics_info[topic_id]['info_hash'] != topic['INFO_HASH']
                added_int_changed = all_topics_info[topic_id]['added_int'] != topic['ADDED_INT']
                user_author_changed = all_topics_info[topic_id]['user_author'] != topic['USER_AUTHOR']
                tor_frozen_changed = all_topics_info[topic_id]['tor_frozen'] != topic['TOR_FROZEN']
                seed_never_seen_changed = all_topics_info[topic_id]['seed_never_seen'] != topic['SEED_NEVER_SEEN']

                updated = topic_title_changed or topic_time_changed or poster_id_changed or forum_id_changed \
                    or tor_status_text_changed or tor_size_int_changed or tor_private_changed or info_hash_changed \
                    or added_int_changed or user_author_changed or tor_frozen_changed or seed_never_seen_changed

                if updated:
                    changed_topics.add(topic_id)
                else:
                    unchanged_topics.add(topic_id)

        d = {}
        for dat in data_from_plab[url]:
            topic_id = dat['TOPIC_ID']
            d[topic_id] = dat


        save_new_topics(run_id, new_topics, d)
        print('                         New topics:       {}'.format(len(new_topics)))
        save_changed_topics(run_id, changed_topics, d)
        print('                         Changed topics:   {}'.format(len(changed_topics)))
        save_unchanged_topics(run_id, unchanged_topics, d)
        print('                         Unchanged topics: {}'.format(len(unchanged_topics)))

    print('{}: Saving (run_id <-> url_id <-> topic_id) to DB'.format(get_now()))
    save_url_topics(url_topics_data_tmp)


def finish_run(run_id, error_text):
    # TODO make different error_types: WARNINGS and ERRORS. Now only ERRORs hardcoded
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    with psycopg2.connect(host=cfg['DEFAULT']['host'],
                          port=int(cfg['DEFAULT']['port']),
                          database=cfg['DEFAULT']['database'],
                          user=cfg['DEFAULT']['user'],
                          password=cfg['DEFAULT']['password']) as conn:
        with conn.cursor() as cur:
            sql_get_run_id = '''UPDATE plab2.runs SET status = %s, end_dttm = %s WHERE run_id = %s;'''
            if error_text is None:
                cur.execute(sql_get_run_id, (0, datetime.datetime.now(), run_id))
                conn.commit()
            else:
                cur.execute(sql_get_run_id, (-1, datetime.datetime.now(), run_id))
                sql_save_errors = '''INSERT INTO plab2.run_errors (run_id, error_type, error_text) VALUES (%s, %s, %s)'''
                cur.execute(sql_save_errors, (run_id, 'ERROR', error_text))
                conn.commit()


def main():
    start_dttm = get_now()

    run_id = get_new_run_id()
    start_new_run(run_id)

    print('Started run_id = {} at {}'.format(run_id, start_dttm))

    urls = get_plab_urls()  # get the list of urls for going to plab

    urls_data = get_urls_data()  # SELECT url_id, url FROM plab2.urls

    status, data_from_plab = get_jsons_from_plab(run_id, urls)  # get jsons from files or from plab


    if status[0] != 0:
        finish_run(run_id, status[1])
        return status, start_dttm
    else:
        save_plab_data(run_id, data_from_plab, urls_data)
        finish_dttm = get_now()

        error_text = None
        finish_run(run_id, error_text)
        print('Finished run_id = {} at {}'.format(run_id, finish_dttm))
        print('Run_id = {} duration: {}'.format(run_id, finish_dttm-start_dttm))
        return status, start_dttm

if __name__ == '__main__':
    while True:
        status, start_dttm = main()
        if status[0] != 0:
            retry_timeout = 60
            print('Have errors in run. \n{}\nTrying to retry in {} seconds'.format(status[1], retry_timeout))
            time.sleep(retry_timeout)
            status, start_dttm_retry = main()
            if status[0] != 0:
                print('Retry failed. Waiting next cycle')
        print('{}: going to sleep'.format(get_now()))
        print('\n\n\n')
        time.sleep(60*60 - int((datetime.datetime.now()-start_dttm).total_seconds()))


# TODO check VPN status (by IP? by trying to go to main plab page? smth else?). Try to start VPN
# TODO catch exceptions and write them to run_errors
# TODO if error - try after some minutes withthe same run_id and warining in logs
# TODO save jsons from plab to disk and ability to process them (in debugger?)
# TODO check if authorization at plab is okay
# TODO start script with scheduler (with venv and correct paths)
# TODO Change names for more mnemonic
# TODO make config files less reread and decrease DB connections (may be only one connection?)
# TODO save txt-logs