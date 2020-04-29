from smart_open import open
with open('s3://up-t-files/dbpedia/t1/dbpedia.txt', 'wb') as fin:
    for line in open('https://downloads.dbpedia.org/repo/dbpedia/text/short-abstracts/2020.02.01/short-abstracts_lang=en.ttl.bz2'):
        fin.write(line.replace('\n','').encode("utf-8")+b'\n')
