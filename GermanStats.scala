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
import java.nio.file.{Files, Path, Paths}

import scala.io.Source


case class GermanNoun(
                       singularNominative: String,
                       gender: String,
                       singularGenitive: Option[String],
                       pluralNominative: Option[String]
                     )


case class GenderStats(
                        nouns: Iterable[GermanNoun]
                      ) {
  require(nouns.nonEmpty)


  val (masculineCount, neuterCount, feminineCount): (Long, Long, Long) = {
    val genderMap =
      nouns
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
  println("== LOADING NOUNS ==")
  println()

  val nouns =
    loadNouns()

  println(nouns.size + " nouns found!")
  println()
  println()


  val outputDirPath = Paths.get("outputStats")
  Files.createDirectories(outputDirPath)


  computeGenderStatsByEnding(
    outputDirPath,
    nouns
  )

  computeGenderStatsByBeginning(
    outputDirPath,
    nouns
  )
}


def computeGenderStatsByEnding(outputDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val genderStatsByEnding =
    computeGenderStatsByGrouping(
      nouns,
      (endingLength, nominative) => nominative.substring(
        nominative.length - endingLength
      )
    )

  println("== COMPUTING GENDER STATS BY ENDING ==")
  println()

  writeGenderStatsByGrouping(
    outputDirPath.resolve("genderStatsByEnding.yml"),
    genderStatsByEnding
  )

  val filteredGenderStatsByEnding =
    compactStats(
      genderStatsByEnding,
      (potentiallyImplying, potentiallyImplied) => potentiallyImplied.endsWith(potentiallyImplying),
      1.0
    )

  writeGenderStatsByGroupingAndRelevance(
    outputDirPath.resolve("genderStatsByEndingAndRelevance.yml"),
    filteredGenderStatsByEnding
  )

  println()
  println()
  println()
}



def computeGenderStatsByBeginning(outputDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val genderStatsByBeginning =
    computeGenderStatsByGrouping(
      nouns,
      (beginningLength, nominative) => nominative.substring(0, beginningLength)
    )

  println("== COMPUTING GENDER STATS BY BEGINNING ==")
  println()

  writeGenderStatsByGrouping(
    outputDirPath.resolve("genderStatsByBeginning.yml"),
    genderStatsByBeginning
  )

  val filteredGenderStatsByBeginning =
    compactStats(
      genderStatsByBeginning,
      (potentiallyImplying, potentiallyImplied) => potentiallyImplied.startsWith(potentiallyImplying),
      1.0
    )

  writeGenderStatsByGroupingAndRelevance(
    outputDirPath.resolve("genderStatsByBeginningAndRelevance.yml"),
    filteredGenderStatsByBeginning
  )

  println()
  println()
  println()
}




def loadNouns(): List[GermanNoun] = {
  val CORPUS_FILE = "corpus.csv"

  val EMPTY_FIELD = "—"


  Source
    .fromFile(CORPUS_FILE)
    .getLines
    .map(corpusLine => {
      val nounElements =
        corpusLine.split(',')

      val singularNominative =
        nounElements(0)

      val gender =
        nounElements(1)

      val singularGenitive =
        nounElements(2) match {
          case `EMPTY_FIELD` =>
            None
          case anyValue =>
            Some(anyValue)
        }


      val pluralNominative =
        nounElements(3) match {
          case `EMPTY_FIELD` =>
            None
          case anyValue =>
            Some(anyValue)
        }


      GermanNoun(
        singularNominative,
        gender,
        singularGenitive,
        pluralNominative
      )
    })
    .toList
}



def computeGenderStatsByGrouping(
                                  nouns: Iterable[GermanNoun],
                                  groupingFunction: (Int, String) => String,
                                  maxGroupingLength: Int = 6,
                                  minGroupedNounsCount: Long = 100
                                ): Map[Int, Map[String, GenderStats]] = {
  Range
    .inclusive(1, maxGroupingLength)
    .map(groupingLength =>
      groupingLength ->
        nouns
          .filter(
            _.singularNominative.length >= maxGroupingLength
          )
          .groupBy(noun => {
            groupingFunction(
              groupingLength,
              noun.singularNominative
            )
          })
          .filter {
            case (grouping, groupedNouns) =>
              groupedNouns.size >= minGroupedNounsCount
          }
          .map {
            case (grouping, groupedNouns) =>
              grouping -> GenderStats(
                groupedNouns
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


def writeGenderStatsByGrouping(
                                outputFilePath: Path,
                                statsMap: Map[Int, Map[String, GenderStats]]
                              ): Unit = {
  val outputWriter = new PrintWriter(Files.newBufferedWriter(outputFilePath))

  try {
    statsMap
      .keys
      .toList
      .sorted
      .foreach(groupingLength => {
        println(s"=== GROUPING NOUNS WITH GROUPING STRINGS OF ${groupingLength} LETTER(S) ===")
        println()

        outputWriter.println(s"${groupingLength}:")


        val groupingStats =
          statsMap(groupingLength)


        groupingStats
          .toList
          .sortBy(_._1)
          .foreach {
            case (groupingString, stats) =>
              writeGenderStats(outputWriter, groupingString, stats, 1)
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


def writeGenderStatsByGroupingAndRelevance(
                                            outputFilePath: Path,
                                            statsMap: Map[Int, Map[String, GenderStats]]
                                          ): Unit = {
  val outputWriter = new PrintWriter(Files.newBufferedWriter(outputFilePath))

  try {
    println(s"=== SORTING STATS BY RELEVANCE ===")
    println()

    val statsSortedByRelevance =
      statsMap
        .values
        .flatten
        .toList
        .sortBy {
          case (groupingString, stats) =>
            stats.relevance
        }
        .reverse

    statsSortedByRelevance
      .foreach {
        case (groupingString, stats) =>
          writeGenderStats(outputWriter, groupingString, stats, 0)
      }

    println()
    println()

  } finally {
    outputWriter.close()
  }
}
