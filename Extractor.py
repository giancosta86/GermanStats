#!/usr/bin/python3
# -*- coding: utf-8 -*-

# ===========================================================================
# GermanStats
# ===========================================================================
# Copyright (C) Gianluca Costa
# ===========================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===========================================================================


import re


nounPattern = re.compile(r"\s*{{Deutsch Substantiv \Sbersicht\s*")
genderPattern = re.compile(r"\s*\|Genus=(\S+)\s*")
singularNominativePattern = re.compile(r"\s*\|Nominativ Singular=(\S+)\s*")
pluralNominativePattern = re.compile(r"\s*\|Nominativ Plural=(\S+)\s*")
singularGenitivePattern = re.compile(r"\s*\|Genitiv Singular=(\S+)\s*")


currentWord = None

sourceFileName = "source.xml"
corpusFileName = "corpus.csv"


SINGULAR_NOMINATIVE_KEY = "singularNominative"
GENDER_KEY = "gender"
SINGULAR_GENITIVE_KEY = "singularGenitive"
PLURAL_NOMINATIVE_KEY = "pluralNominative"


with open(sourceFileName) as sourceFile:
    with open(corpusFileName, "w") as corpusFile:
        for line in sourceFile:
            nounMatcher = nounPattern.match(line)

            if nounMatcher is not None:
                currentWord = {}

            elif currentWord is not None:
                genderMatcher = genderPattern.match(line)

                if genderMatcher is not None:
                    currentWord[GENDER_KEY] = genderMatcher.group(1).lower()

                else:
                    singularNominativeMatcher = singularNominativePattern.match(line)

                    if (singularNominativeMatcher is not None):
                        currentWord[SINGULAR_NOMINATIVE_KEY] = singularNominativeMatcher.group(1).title()

                    else:
                        pluralNominativeMatcher = pluralNominativePattern.match(line)

                        if (pluralNominativeMatcher is not None):
                            currentWord[PLURAL_NOMINATIVE_KEY] = pluralNominativeMatcher.group(1).title()

                        else:
                            singularGenitiveMatcher = singularGenitivePattern.match(line)

                            if (singularGenitiveMatcher is not None):
                                currentWord[SINGULAR_GENITIVE_KEY] = singularGenitiveMatcher.group(1).title()

                                singularNominative = currentWord.get(SINGULAR_NOMINATIVE_KEY)
                                gender = currentWord.get(GENDER_KEY, "—")
                                pluralNominative = currentWord.get(PLURAL_NOMINATIVE_KEY, "—")
                                singularGenitive = currentWord.get(SINGULAR_GENITIVE_KEY, "—")

                                isValidWord = (
                                    (singularNominative is not None)
                                    and (singularNominative not in ["", "-", "—"])
                                    and (gender in ["m", "n", "f"])
                                )

                                if isValidWord:
                                    currentWordString = ",".join([
                                        singularNominative,
                                        gender,
                                        singularGenitive,
                                        pluralNominative
                                    ])

                                print(currentWordString)
                                corpusFile.write(currentWordString + "\n")
                                currentWord = None
