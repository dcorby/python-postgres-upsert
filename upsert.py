import psycopg2

to_tup = lambda dct, k: tuple([dct[x] for x in k])

""" 
args
----
db : db
  database class with methods get(), get_fields(), sql(), and insert()
table : str
  table name as string
keys : list
  list of tuples representing unique keys
upserts : list
  rows to upsert as a list of dicts

kwargs
------
  where : str
    limiting clause for current rows
  delete : bool
    delete unmatched rows
  ignore : list
    list of fields to ignore on update
  default : dict
    dict of values and defaults to set on insert
  noinsert : bool
    do not insert unmatched rows
  get_unmatched : bool
    return unmatched rows
  overwrite : bool
    set field to null if field not contained in upsert
  ignorenull : bool
    do not set fields to null
  before_insert : list
    hook to run function prior to insert ([func, *args])
  nonullkeys : bool
    prevent match on keys will null field values
  keymaps : dict
    dict of key mappings to make on match 
  dryrun : bool
    print planned database operations to stdout
"""

def upsert(db, table, keys, upserts, **kwargs):

    def parse_keys(keys):
        """ parse keys and return as a list of tuples """
        if not isinstance(keys, list):
            keys = [keys]
        for i, key in enumerate(keys):
            # backwards compatible for extant scalar args
            if type(key) is not tuple:
                keys[i] = tuple([key])
        return keys

    def get_current(db, table, keys, kwargs):
        """ collect current rows with a map of keys to row indexes """
        where = kwargs.get("where")
        rows = db.get(f"SELECT * FROM {table} { ' WHERE ' + where if where else '' }")
        current = { "rows": rows, "tups": {} }
        for i, row in enumerate(rows):
            for key in keys:
                tup = to_tup(row, key)
                tups = [tup]
                # implement keymaps
                for field in  kwargs.get("keymaps", {}):
                    for k in kwargs["keymaps"][field]:
                        idx = key.index(field)
                        if tup[idx] == k:
                            for v in kwargs["keymaps"][field][k]:
                                alt = list(tup)
                                alt[idx] = v
                                tups.append(tuple(alt))
                for tup in tups:
                    if tup not in current["tups"]:
                        current["tups"][tup] = []
                    current["tups"][tup].append(i)
        return current

    def get_to_update(current, upserts, keys, kwargs):
        """ iterate the upsert rows and collect matched rows, with indexes """
        to_update = []
        for j, upsert in enumerate(upserts):
            for key in keys:
                tup = to_tup(upsert, key)
                if tup in current["tups"]:
                    if kwargs.get("nonullkeys") and None in tup:
                        continue
                    for i in current["tups"][tup]:
                        to_update.append({ "key": key, "current": i, "upsert": j })
        return to_update

    def get_default(default, row, field):
        func = default[field][0]
        args = []
        for arg in default[field][1:]:
            if isinstance(arg, str) and arg.startswith("*"):
                args.append(row[arg[1:]])
            else:
                args.append(arg)
        val = func(*args)
        return val

    def do_updates(db, table, to_update, current, upserts, kwargs):
        """ perform the updates, raise type errors and catch uniqueness violations from the table """
        fields = db.get_fields(table)
        ignore = kwargs.get("ignore", [])
        fields = [x for x in fields if x not in ignore]
        for update in to_update:
            rows = { "current": current["rows"][update["current"]], "upsert": upserts[update["upsert"]] }

            # set defaults
            default = kwargs.get("default", {}) 
            if default:
                for field in default:
                    rows["upsert"][field] = get_default(default, rows["upsert"], field)

            sets, args = [], []
            for field in fields:
                cur_val = rows["current"][field]
                if not kwargs.get("overwrite") and field not in rows["upsert"]:
                    continue
                new_val = rows["upsert"].get(field)
                if kwargs.get("ignorenull") and new_val is None:
                    continue
                if cur_val is not None and new_val is not None and type(cur_val) != type(new_val):
                    msg = f"can't perform upsert for field={field}; types differ for new={new_val} {type(new_val)} and cur={cur_val} {type(cur_val)}"
                    raise TypeError(msg)
                if cur_val != new_val:
                    sets.append(f"{field} = %s")
                    args.append(new_val)
            if sets:
                where = [kwargs["where"]] if kwargs.get("where") else []
                for field in update["key"]:
                    val = rows["current"][field]
                    where.append(f"{field} { 'IS' if val is None else '=' } %s")
                    args.append(val)
                query = f"UPDATE {table} SET { ', '.join(sets) } WHERE { ' AND '.join(where) }"
                try:
                    if kwargs.get("dryrun"):
                        print(query, tuple(args))
                    else:
                        db.sql(query, tuple(args))
                except psycopg2.errors.UniqueViolation as e:
                    print(e)

    def get_unmatched(to_update, current, upserts):
        """ collect unmatched rows from current and upserts """
        indexes = { "current": set(), "upserts": set() }
        for update in to_update:
            indexes["current"].add(update["current"])
            indexes["upserts"].add(update["upsert"])
        
        unmatched = { "current": set(), "upserts": set() }
        for i, _ in enumerate(current["rows"]):
            if i not in indexes["current"]:
                unmatched["current"].add(i)
        for i, _ in enumerate(upserts):
            if i not in indexes["upserts"]:
                unmatched["upserts"].add(i)
        return unmatched

    def do_inserts(db, table, unmatched, upserts, kwargs):
        """ insert the unmatched rows, set defaults and run hooks as required """
        for i in unmatched["upserts"]:
            row = upserts[i]

            # set defaults
            default = kwargs.get("default", {}) 
            if default:
                for field in default:
                    row[field] = get_default(default, row, field)

            # before hook
            before = kwargs.get("before_insert", [])
            if before:
                args = []
                for arg in before[1:]:
                    if isinstance(arg, str) and arg.startswith("*"):
                        args.append(row[arg[1:]])
                    else:
                        args.append(arg)
                if not kwargs.get("dryrun"):
                    before[0](*args)

            try:
                if kwargs.get("dryrun"):
                    print("INSERT", row)
                else:
                    db.insert(row, table)
            except psycopg2.errors.UniqueViolation as e:
                print(e)

    def do_deletes(db, table, keys, current, unmatched, kwargs):
        """ delete unmatched current rows """
        for i in unmatched["current"]:
            for key in keys:
                row = current["rows"][i]
                tup = to_tup(row, key)
                where = [kwargs["where"]] if kwargs.get("where") else ["1=1"]
                args = []
                for field in key:
                    val = row[field]
                    where.append(f"{field} { 'IS' if val is None else '=' } %s")
                    args.append(val)
                query = f"DELETE FROM {table} WHERE { ' AND '.join(where) }"
                if kwargs.get("dryrun"):
                    print(query, tuple(args))
                else:
                    db.sql(query, tuple(args))

    """ process rows, do operations, and return unmatches if necessary """
    keys = parse_keys(keys)
    current = get_current(db, table, keys, kwargs)
    to_update = get_to_update(current, upserts, keys, kwargs)
    do_updates(db, table, to_update, current, upserts, kwargs)
    unmatched = get_unmatched(to_update, current, upserts)
    if not kwargs.get("noinsert"):
        do_inserts(db, table, unmatched, upserts, kwargs)
    if kwargs.get("delete"):
        do_deletes(db, table, keys, current, unmatched, kwargs)
    if kwargs.get("get_unmatched"):
        return [upserts[i] for i in unmatched["upserts"]]
    return True

