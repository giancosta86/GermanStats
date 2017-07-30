/*
  ===========================================================================
  GermanStats
  ===========================================================================
  Copyright (C) Gianluca Costa
  ===========================================================================
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  ===========================================================================
*/

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import scala.io.Source


case class GermanWord(
                       singularNominative: String,
                       gender: String,
                       singularGenitive: Option[String],
                       pluralNominative: Option[String]
                     )


case class GenderStats(
                        words: Iterable[GermanWord]
                      ) {
  require(words.nonEmpty)


  val (masculineCount, neuterCount, feminineCount): (Long, Long, Long) = {
    val genderMap =
      words
        .groupBy(_.gender)

    (
      genderMap.getOrElse("m", List()).size,
      genderMap.getOrElse("n", List()).size,
      genderMap.getOrElse("f", List()).size
    )
  }

  val totalCount: Long = masculineCount + neuterCount + feminineCount


  def masculinePercentage: Double =
    masculineCount * 100.0 / totalCount


  def neuterPercentage: Double =
    neuterCount * 100.0 / totalCount


  def femininePercentage: Double =
    feminineCount * 100.0 / totalCount


  val mainGender: String =
    if (masculinePercentage > neuterPercentage) {
      if (masculinePercentage > femininePercentage)
        "m"
      else
        "f"
    } else {
      if (neuterPercentage > femininePercentage)
        "n"
      else
        "f"
    }


  val highestPercentage: Double =
    mainGender match {
      case "m" =>
        masculinePercentage

      case "f" =>
        femininePercentage

      case "n" =>
        neuterPercentage
    }


  val relevance: Long =
    (1e3 * highestPercentage + totalCount).toLong


  override def toString: String =
    f"(M: ${masculinePercentage}%.2f%%, N: ${neuterPercentage}%.2f%%, F: ${femininePercentage}%.2f%%; G: ${mainGender}; T: ${totalCount}%d; R: ${relevance}%d)"

}


mainFunction()


def mainFunction() {
  println("Loading words...")

  val words =
    loadWords()

  println(words.size + " words found!")

  val genderStatsByEnding =
    computeGenderStatsByEnding(words)

  writeGenderStatsByEnding(genderStatsByEnding)

  val filteredGenderStatsByEnding =
    compactStats(
      genderStatsByEnding,
      (potentiallyImplying, potentiallyImplied) => potentiallyImplied.endsWith(potentiallyImplying),
      1.0
    )

  writeGenderStatsByEndingAndRelevance(filteredGenderStatsByEnding)
}




def loadWords(): List[GermanWord] = {
  val CORPUS_FILE = "corpus.csv"

  val EMPTY_FIELD = "—"


  Source
    .fromFile(CORPUS_FILE)
    .getLines
    .map(corpusLine => {
      val wordElements =
        corpusLine.split(',')

      val singularNominative =
        wordElements(0)

      val gender =
        wordElements(1)

      val singularGenitive =
        wordElements(2) match {
          case `EMPTY_FIELD` =>
            None
          case anyValue =>
            Some(anyValue)
        }


      val pluralNominative =
        wordElements(3) match {
          case `EMPTY_FIELD` =>
            None
          case anyValue =>
            Some(anyValue)
        }


      GermanWord(
        singularNominative,
        gender,
        singularGenitive,
        pluralNominative
      )
    })
    .toList
}



def computeGenderStatsByEnding(words: Iterable[GermanWord]): Map[Int, Map[String, GenderStats]] = {
  val MAX_ENDING_LENGTH = 6
  val MIN_GROUPED_WORDS_COUNT = 100

  Range
    .inclusive(1, MAX_ENDING_LENGTH)
    .map(endingLength =>
      endingLength ->
        words
          .filter(
            _.singularNominative.length >= MAX_ENDING_LENGTH
          )
          .groupBy(word => {
            val nominative =
              word.singularNominative

            val ending =
              nominative.substring(
                nominative.length - endingLength
              )

            ending
          })
          .filter {
            case (ending, groupedWords) =>
              groupedWords.size >= MIN_GROUPED_WORDS_COUNT
          }
          .map {
            case (ending, groupedWords) =>
              ending -> GenderStats(
                groupedWords
              )
          }
    )
    .toMap
}


def compactStats(
                  statsMap: Map[Int, Map[String, GenderStats]],
                  implicationOnGroupingStrings: (String, String) => Boolean,
                  maxHighestPercentageDeltaForImplication: Double
                ): Map[Int, Map[String, GenderStats]] = {
  statsMap
    .map {
      case (groupingStringLength, groupingStatsMap) =>
        val flatStatsList: Iterable[(String, GenderStats)] =
          statsMap
            .filter {
              case (flatGroupingStringLength, _) =>
                flatGroupingStringLength < groupingStringLength
            }
            .values
            .flatten

        val filteredGroupingStatsMap =
          groupingStatsMap.filter {
            case (groupingString, groupingStats) =>
              val isGroupingImplied =
                flatStatsList.exists {
                  case (flatGroupingString, flatStats) =>
                    implicationOnGroupingStrings(flatGroupingString, groupingString) &&
                      (flatStats.totalCount >= groupingStats.totalCount) &&
                      (flatStats.highestPercentage + maxHighestPercentageDeltaForImplication >= groupingStats.highestPercentage)
                }

              !isGroupingImplied
          }

        groupingStringLength -> filteredGroupingStatsMap
    }
    .filter {
      case (groupingStringLength, groupingStatsMap) =>
        groupingStatsMap.nonEmpty
    }
}


def writeGenderStatsByEnding(statsMap: Map[Int, Map[String, GenderStats]]): Unit = {
  val STATS_FILE = "genderStatsByEnding.yml"

  val outputWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(STATS_FILE)))

  try {
    statsMap
      .keys
      .toList
      .sorted
      .foreach(endingLength => {
        println(s"*** GROUPING WORDS ACCORDING TO THE LAST ${endingLength} LETTER(S) ***")
        println()

        outputWriter.println(s"${endingLength}:")


        val groupingStats =
          statsMap(endingLength)


        groupingStats
          .toList
          .sortBy(_._1)
          .foreach {
            case (ending, stats) =>
              writeGenderStats(outputWriter, ending, stats, 1)
          }

        println()
        println()
      })
  } finally {
    outputWriter.close()
  }
}



def writeGenderStats(outputWriter: PrintWriter, groupingString: String, stats: GenderStats, leadingSpacesCount: Int): Unit = {
  println(s"${groupingString} --> ${stats}")

  val leadingSpaces =
    " " * leadingSpacesCount

  outputWriter.println(s"${leadingSpaces}${groupingString}:")
  outputWriter.println(f"${leadingSpaces} masculinePercentage: ${stats.masculinePercentage}%.2f")
  outputWriter.println(f"${leadingSpaces} neuterPercentage: ${stats.neuterPercentage}%.2f")
  outputWriter.println(f"${leadingSpaces} femininePercentage: ${stats.femininePercentage}%.2f")
  outputWriter.println(s"${leadingSpaces} totalCount: ${stats.totalCount}")
  outputWriter.println(s"${leadingSpaces} relevance: ${stats.relevance}")
  outputWriter.println(s"${leadingSpaces} mainGender: ${stats.mainGender}")
  outputWriter.println()
}


def writeGenderStatsByEndingAndRelevance(statsMap: Map[Int, Map[String, GenderStats]]): Unit = {
  val STATS_FILE = "genderStatsByEndingAndRelevance.yml"

  val outputWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(STATS_FILE)))

  try {
    println(s"*** SORTING STATS BY RELEVANCE ***")

    val statsSortedByRelevance =
      statsMap
        .values
        .flatten
        .toList
        .sortBy {
          case (ending, stats) =>
            stats.relevance
        }
        .reverse

    statsSortedByRelevance
      .foreach {
        case (ending, stats) =>
          writeGenderStats(outputWriter, ending, stats, 0)
      }

    println()
    println()

  } finally {
    outputWriter.close()
  }
}
