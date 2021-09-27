#!/usr/bin/env python3

import pandas as pd

def sample_all_sizes(name, dataset):
    for size in dataset_sizes:
        n_rows = round(dataset.shape[0] * size)
        sampled_df = dataset.sample(n=n_rows, random_state=42, replace=True)
        filename =  name + '{}.tsv'.format(int(size*2048))
        sampled_df.to_csv(filename, index=False, sep='\t')


dataset_sizes = [0.125, 0.25, 0.5, 2]

title_basics = pd.read_csv('title_basics.tsv', sep='\t')
title_akas = pd.read_csv('title_akas.tsv', sep='\t')
name_basics = pd.read_csv('name_basics.tsv', sep='\t')
title_principals = pd.read_csv('title_principals.tsv', sep='\t')
title_ratings = pd.read_csv('title_ratings.tsv', sep='\t')

sample_all_sizes("title_basics", title_basics)
sample_all_sizes("title_akas", title_akas)
sample_all_sizes("name_basics", name_basics)
sample_all_sizes("title_principals", title_principals)
sample_all_sizes("title_ratings", title_ratings)
