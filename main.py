import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)

   return conn

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_team(conn, team):
   """
   Create a new team into the teams table
   :param conn:
   :param team:
   :return: team id
   """
   sql = '''INSERT INTO teams(name, match_played, won, draw, lost, goals_scored, goals_lost, goals_balance, points, league_position)
             VALUES(?,?,?,?,?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, team)
   conn.commit()
   return cur.lastrowid

def add_player(conn, player):
   """
   Create a new player into the players table
   :param conn:
   :param player:
   :return: player id
   """
   sql = '''INSERT INTO players(team_id, name, age, description)
             VALUES(?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, player)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   """
   Delete all rows from table
   :param conn: Connection to the SQLite database
   :param table: table name
   :return:
   """
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == "__main__":
   
   create_teams_sql = """
   -- teams table
   CREATE TABLE IF NOT EXISTS teams (
      id integer PRIMARY KEY,
      name text NOT NULL,
      match_played integer NOT NULL,
      won integer NOT NULL,
      draw integer NOT NULL,
      lost integer NOT NULL,
      goals_scored integer NOT NULL,
      goals_lost integer NOT NULL,
      goals_balance integer NOT NULL,
      points integer NOT NULL,
      league_position integer NOT NULL
   );
   """

   create_players_sql = """
   -- players table
   CREATE TABLE IF NOT EXISTS players (
      id integer PRIMARY KEY,
      team_id integer NOT NULL,
      name VARCHAR(250) NOT NULL,
      age integer NOT NULL,
      description text NOT NULL,
      FOREIGN KEY (team_id) REFERENCES teams (id)
   );
   """

   #create and connect
   db_file = "db\database.db"
   conn = create_connection(db_file)
   if conn is not None:
       execute_sql(conn, create_teams_sql)
       execute_sql(conn, create_players_sql)
       #conn.close()
   team = ("Arsenal", "3", "1", "1", "1", "5", "4", "1", "4", "9")
   team_id = add_team(conn, team) 
   player = (team_id, "John Cleese", "82", "Obiecujący talent")
   player_id = add_player(conn, player)

   #read
   players = select_all(conn, "players")
   print(players)

   #update
   update(conn, "players", 1, description="Przeszedł na emeryturę")
   
   #delete
   delete_where(conn, "players", id=1)

   conn.close()
