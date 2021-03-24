import psycopg2
import _thread
import time
import threading


def delete_part(limit, offset):
    """ delete part by part id """
    rows_deleted = 0
    try:
        start_time = time.time()

      
        # create a new cursor
        cur = conn.cursor()

        row = []
        cur2 = conn.cursor()
        sql2 = u"SELECT work_order_id, work_order_date FROM work_order WHERE work_order_id IN (SELECT work_order_id FROM work_order where work_order_date < '2020-01-01' order by work_order_id asc limit '%s' offset '%s')"
        vars2 = limit, offset
        cur2.execute(sql2, vars2)
        # cur2.execute("SELECT work_order_id, summary_date FROM work_order_branch_summary where work_order_id < '88599337' order by work_order_id asc limit ", limit, " offset ", offset)
        while row is not None:
            print(row)
            row = cur2.fetchone()

        # execute the UPDATE  statement
        sql = u"DELETE FROM work_order WHERE work_order_id IN (SELECT work_order_id FROM work_order where work_order_date < '2020-01-01'  order by work_order_id asc  limit '%s' offset '%s')"
        print(u"DELETE FROM work_order WHERE work_order_id IN (SELECT work_order_id FROM work_order where work_order_date < '2020-01-01'  order by work_order_id asc limit", limit, " offset ",  offset, ")")
        print()
        vars = limit, offset
        cur.execute(sql, vars)
        #cur.execute("DELETE FROM work_order WHERE work_order_id IN (SELECT work_order_id FROM work_order where work_order_date < '2020-01-01' limit 10 offset 1)")

        # get the number of updated rows
        rows_deleted = cur.rowcount
        print(rows_deleted)
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database

        cur2.close()
        cur.close()
        print("--- %s seconds ---" % (time.time() - start_time))
        print("DELETED SUCCESS")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return rows_deleted

def exec():
    try:
        delete_part(1, 0)
        #delete_part(10, 11)
    except:
        print("ERROR : unable start")


if __name__ == '__main__':
    threads = []
    t = threading.Thread(target=exec())
    threads.append(t)
    t.start()
