import pandas as pd
import collections as ctn
from re import findall


def count_common_words(filename: str):
    """
      count common words from the supplied json file per row, create an identity column,
        and output the result data into a csv file.
      """
    print("***** File count processing begins *****")
    # read json file into a variable
    read_json = pd.read_json(filename)

    # normalize and write the read json object to a dataframe
    df_abstract = pd.json_normalize(read_json['abstract']).rename(columns={"_value": "petitions"})

    # read the dataframe as text into a variable
    new_string = df_abstract.to_string().lower()
    write_string = findall(r"\w*", new_string)

    print(" ***** Removing common used words that are less than 5 letters *****")
    # store common words with length greater than 4 into a list
    split_it = [i for i in write_string if len(i) > 4]

    # count unique word store in the list
    counter = ctn.Counter(split_it)

    print("***** Getting 20 of the most commonly used words *****")
    # store only 20 from the common words into a variable
    most_occur = counter.most_common(20)

    # unpack the common most words to separate the words from the total count
    common_words, count = zip(*most_occur)

    print("***** Printing the commonly used words to the screen *****")
    common_words = list(common_words)
    print(common_words)
    no_of_occurrence_in_petition = []

    # get the row count of the entire record
    row_count = df_abstract.count()["petitions"]

    print("***** looping through each common word and count its number of occurrence in each row *****")
    # loop through each common word and count its number of occurrence in each row
    for word in common_words:
        for index, row in df_abstract.iterrows():
            no_of_occurrence_in_petition.append(row["petitions"].strip().lower().count(word))
            if row_count == len(no_of_occurrence_in_petition):
                df_abstract[word] = no_of_occurrence_in_petition
                no_of_occurrence_in_petition.clear()

    # generate a dumping column group
    df_abstract["drv_col"] = ["aviva"] * row_count

    print("***** Generating a unique primary key column *****")
    # generate a unique primary key for the final output
    df_abstract["petition_id"] = df_abstract.sort_values(["petitions"], ascending=True).groupby(
        ["drv_col"]).cumcount() + 1

    # re-order column position to put primary key at the first position
    df_abstract.insert(0, 'petition_id', df_abstract.pop('petition_id'))
    processed_data = 'count_common_words_per_row.csv'

    # drop needless columns and write final data to a csv file
    print("***** Saving processed data as a csv file *****")
    df_abstract.drop(columns=["petitions", "drv_col"]) \
        .sort_values(["petition_id"], ascending=True) \
        .to_csv(processed_data, index=False)

    print("***** a new file named %s has been generated *****" % processed_data)
    print("***** File count processing ends *****")
