#!/usr/bin/python3
# -*- coding: utf-8 -*-
#encoding: utf-8
import config
import telebot
import utils
import logging
import random
import string
import os
import gzip
import shutil
import psycopg2
import datetime
import sys
import json
import subprocess

from telebot import types
from time import sleep

bot = telebot.TeleBot(config.token)
query_dict = {}
admin_list = []


FORMAT = "%(asctime)-15s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler(config.log_file, encoding = "UTF-8")
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

class Query:
    def __init__(self, db):
        self.user_info = None
        self.db = db
        self.type = None
        self.text = None
        self.uid = None


@bot.message_handler(commands=['help'])
def print_help(message):
    bot.send_message(message, 'Команды:\n' +
                     '/db [dbname] - выбор базы данных\n' +
                     '/dblist - список баз данных\n' +
                     '/list - вывести список запросов, ожидающих подтверждения и их id\n' +
                     '/approve foo bar - выполнить (и удалить) запросы с id "foo" и "bar" находящиеся в очереди\n' +
                     '/decline foo bar - удалить из очереди запросы с id "foo" и "bar"\n\n' +
                     'Пример:\n' +
                     '/select [Enter]\n' +
                     'SELECT * FROM tablename;\n')

@bot.message_handler(commands=['my_id'])
def my_id(message):
    bot.send_message(message.chat.id, 'Chat id: ' + str(message.chat.id))

def check_vars(message):
    if message.from_user.first_name is None:
        message.from_user.first_name = ''
    if message.from_user.last_name is None:
        message.from_user.last_name = ''
    if message.from_user.username is None:
        message.from_user.username = ''
    return message

def get_role(message):
    if message.from_user.username in config.allowed_users:
        role = config.allowed_users.get(message.from_user.username)
    elif message.from_user.id in config.allowed_users_id:
        role = config.allowed_users_id.get(message.from_user.id)
    return role

def check_auth(message):
    if (message.from_user.username in config.allowed_users or message.from_user.id in config.allowed_users_id):
        return True
    else:
        return False

@bot.message_handler(commands=['start'])
def start(message):
    message = check_vars(message)
    auth = check_auth(message)
    if not auth:
        bot.send_message(message.chat.id, 'Нужна авторизация!')
        logger.warning(message.from_user.first_name + \
                       ' ' + message.from_user.last_name + \
                       ' [' + message.from_user.username + '] ' + \
                       ' (' + str(message.from_user.id) + ') - access required')
        bot.send_message('-213221690', message.from_user.first_name + ' ' + \
                         message.from_user.last_name + ' ' + \
                         '[' + message.from_user.username + '] ' + \
                         '(' + str(message.from_user.id) + ') \n' + \
                         'Хочет авторизоваться но не может!')
        return
    role = get_role(message)
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    for key, val in sorted(config.db_ro.items()):
        if val == 'all' or val == role:
            markup.add(key)
    msg = bot.reply_to(message, 'Выберите БД', reply_markup=markup)
    message = check_vars(message)
    logger.info('Access: ' + message.from_user.first_name + \
                ' ' + message.from_user.last_name + \
                ' [' + message.from_user.username + '] ' + \
                ' (' + str(message.from_user.id) + ')')
    bot.register_next_step_handler(msg, select_db)


def select_db(message):
    chat_id = message.chat.id
    db_name = message.text
    if db_name not in config.db_ro:
        msg = bot.reply_to(message, 'Эй, только базы из спиcка можно!')
        bot.register_next_step_handler(msg, select_db)
        return
    query = Query(db_name)
    query_dict[chat_id] = query
    query.uid = message.from_user.id
    query.user_info = message.from_user.id
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True)
    markup.add('select', 'update')
    msg = bot.reply_to(message, 'Выберите тип запроса:', reply_markup=markup)
    bot.register_next_step_handler(msg, select_query_type)


def select_query_type(message):
    chat_id = message.chat.id
    query_type = message.text
    if query_type not in ('select', 'update'):
        msg = bot.reply_to(message, 'Только эти два запроса!')
        bot.register_next_step_handler(msg, select_query_type)
        return
    query = query_dict[chat_id]
    query.type = query_type
    msg = bot.reply_to(message, 'Введите текст запроса')
    bot.register_next_step_handler(msg, get_query_text)


def get_query_text(message):
    chat_id = message.chat.id
    message = check_vars(message)
    text = message.text.split(';')
    query = query_dict[chat_id]
    if text == '':
        msg = bot.reply_to(message, 'Эй, запрос должен содержать текст запроса!')
        bot.register_next_step_handler(msg, get_query_text)
        return
    query.text = text[0]
    bot.send_message(message.chat.id, 'Запрос ушел в обработку...')
    # log to file
    logger.info(str(datetime.datetime.now()) + ': user : ' + \
                message.from_user.first_name + \
                ' ' + message.from_user.last_name + \
                ' [' + message.from_user.username + '] ' + \
                ' (' + str(message.from_user.id) + ')' + \
                ' : db : ' + query.db + \
                ' : query :\n' + query.text)
    # log to security telegram
    log_msg = message.from_user.first_name + ' ' + \
                     message.from_user.last_name + ' ' + \
                     '[' + message.from_user.username + '] ' + \
                     '(' + str(message.from_user.id) + ') \n' + \
                     'Db: ' + query.db + '\n' + \
                     message.text
    bot.send_message('-444444444444', log_msg) # логирование в телеграм чатик безопасников
    # log to scribe
    json_msg = dict(UserDetail=message.from_user.first_name + ' ' + message.from_user.last_name,
                    UserName=message.from_user.username,
                    UserId=message.from_user.id,
                    Query=message.text,
                    Db=query.db
                    )
    # костыль для логирования в скрайб. т.к. рабочей скрайб либы для 3 питона нет, логирование происходит через внешний скрипт на 2 питоне
    try:
        p1 = subprocess.Popen(['echo', json.dumps(json_msg)], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['scribecat', '-h', config.scribe_host, config.scribe_cat], stdin=p1.stdout)
        p1.stdout.close()
        p2.communicate()
        if p2.returncode != 0:
            logger.warning('Shit happened with scribe util')
    except OSError as e:
        logger.error(e.strerror)
    check_answer(query)


@bot.message_handler(commands=['list'])
def list_queries(message):
    message = check_vars(message)
    auth = check_auth(message)
    if not auth:
        bot.send_message(message.chat.id, 'Нужна авторизация!')
        return
    if get_role(message) != 'adm':
        bot.send_message(message.chat.id, 'Имеешь ли ты право?')
        return
    q_list = utils.list_queries()
    for key in q_list:
        bot.send_message(message.chat.id, key + ': [' + q_list.get(key)[1] + ']\n' + q_list.get(key)[0])


@bot.message_handler(commands=['approve'])
def confirm_query(message):
    message = check_vars(message)
    auth = check_auth(message)
    if not auth:
        bot.send_message(message.chat.id, 'Нужна авторизация!')
        return
    if get_role(message) != 'adm':
        bot.send_message(message.chat.id, 'Имеешь ли ты право?')
        return
    parsed = message.text.split(' ')
    for req in parsed[1:]:
        response = utils.confirm_request(req)
        if isinstance(response, psycopg2.Error):
            bot.send_message(query.uid, response.pgerror)
            return
        bot.send_message(message.chat.id, response)


@bot.message_handler(commands=['decline'])
def delete_query(message):
    message = check_vars(message)
    auth = check_auth(message)
    if not auth:
        bot.send_message(message.chat.id, 'Нужна авторизация!')
        return
    if get_role(message) != 'adm':
        bot.send_message(message.chat.id, 'Имеешь ли ты право?')
        return
    parsed = message.text.split(' ')
    for req in parsed[1:]:
        response = utils.remove_request(req)
        bot.send_message(message.chat.id, response)

@bot.message_handler(commands=['restart'])
def restart(message):
    message = check_vars(message)
    auth = check_auth(message)
    if not auth:
        bot.send_message(message.chat.id, 'Нужна авторизация!')
        return
    if get_role(message) == 'adm':
        bot.send_message(message.chat.id, "Bot is restarting...")
        sleep(0.2)
        os.execl(sys.executable, sys.executable, *sys.argv)
    else:
        bot.send_message(message.chat.id, 'Имеешь ли ты право?')
        return

def check_answer(query):
    if query.type == 'select':
        if not query.text.lower().startswith('select'):
            bot.send_message(query.uid, 'Запрос че-то как-то не select')
            return
        col_names, reply = utils.do_pg_select(query.text, query.db)
        if isinstance(reply, psycopg2.Error):
            bot.send_message(query.uid, reply.pgerror)
            return
        format_response(col_names, reply, query.uid)
    elif query.type == 'update':
        if not query.text.lower().startswith('update'):
            bot.send_message(query.uid, 'Запрос че-то как-то не update')
            return
        push_request_to_queue(query)

def format_response(col_names, reply, chat_id):
    fname = '/tmp/' + str(datetime.datetime.now()) + '.csv'
    gz_fname = fname + '.gz'
    fd_w = open(fname, 'wb')
    for col in col_names:
        fd_w.write(col.encode('utf-8') + '|'.encode('utf-8'))
    fd_w.write('\n'.encode('utf-8'))
    if reply[0] != '0':
        for string in reply:
            for val in string:
                fd_w.write(str(val).encode('utf-8') + '|'.encode('utf-8'))
            fd_w.write('\n'.encode('utf-8'))
    fd_w.close()
    file_size = os.path.getsize(fname)
    if file_size > 2097152:
        with open(fname, 'rb') as f_in, gzip.open(gz_fname, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        fd_o = open(gz_fname, 'rb')
        print(str(datetime.datetime.now()) + ': query done')
        bot.send_document(chat_id, fd_o)
        fd_o.close()
        try:
            os.remove(gz_fname)
            os.remove(fname)
        except OSError:
            pass
    else:
        fd_o = open(fname, 'rb')
        print(str(datetime.datetime.now()) + ': query done')
        bot.send_document(chat_id, fd_o)
        fd_o.close()
        try:
            os.remove(fname)
        except OSError:
            pass


def push_request_to_queue(query):
    req_id = utils.add_request_id(query.text, query.db)
    send_notify(req_id, query)


def send_notify(req_id, query):
    bot.send_message(query.uid, 'Ваш запрос ожидает подтверждения')


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def init_admin_list():
    for user in config.allowed_users:
        if config.allowed_users.get(user) == 'adm':
            admin_list.append(user)

if __name__ == '__main__':
    logger.info(str(datetime.datetime.now()) + ' - Tbot started')
    init_admin_list()
    bot.polling(none_stop=True)
