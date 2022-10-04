import logging
import spacy
spacy.prefer_gpu()
nlp = spacy.load("en_core_web_sm") # Choose efficiency over accuracy

def extract_facts_from_description(descriptions):
    facts = [] # fact = [COL]{}[VAL]{}...[COL]{}[VAL]{}

    docs = nlp.pipe(descriptions, disable=['textcat'])
    for doc in docs:
        noun_chunks = doc.noun_chunks
        fact = ''
        for noun_entity in noun_chunks:
            if noun_entity.root.dep_ == 'dobj': # if the subject is equivalent with the head entity? => lexical chain?
                # Lemma: The base form of the word
                # Ref: https://spacy.io/usage/linguistic-features#dependency-parse
                fact = "{}[COL]{}[VAL]{}".format(fact, noun_entity.root.head.lemma_, noun_entity.text)
        facts.append(fact)
    return facts

