import tools.utils.log as log


def print_vals_in_column(df, column, max=20):
    strout = ""
    for i in df.index:
        strout = strout + str(df.at[i, column]) + "\n"
        if i >= max:
            break

    log.getLogger().info(strout)
    return strout


def col_names(df, df_name=""):
    colNames = df.columns.values
    cnstr = ""
    for cn in colNames:
        cnstr = str(cnstr) + "\n" + str(cn)

    log.getLogger().info(df_name + " Column Names: " + cnstr + "\n")


def dfToList( df):
    corpora_list = []

    for i in df.index:
        row_dict ={}
        for col_name in df.columns:
            # log.getLogger().info(self.df.at[i, 'source'])
            row_dict[col_name] = df.at[i, col_name]
        # log.getLogger().debug("Sample text " + cleaned)
        corpora_list.append(row_dict)
    return corpora_list
