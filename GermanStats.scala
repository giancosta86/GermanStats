
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

import scala.annotation.tailrec
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


  val outputDirPath = Paths.get("output/stats")
  Files.createDirectories(outputDirPath)


  computeGender(
    outputDirPath,
    nouns
  )


  computePlurals(
    outputDirPath,
    nouns
  )
}


def computeGender(outputDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val genderDirPath =
    outputDirPath.resolve("gender")

  Files.createDirectories(genderDirPath)


  computeGenderStatsByEnding(
    genderDirPath,
    nouns
  )


  computeGenderStatsByBeginning(
    genderDirPath,
    nouns
  )
}


def computeGenderStatsByEnding(genderDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val genderStatsByEnding =
    computeGenderStatsByGrouping(
      nouns,
      (endingLength, nominative) => getEnding(endingLength, nominative)
    )

  println("== COMPUTING GENDER STATS BY ENDING ==")
  println()

  writeGenderStatsByGrouping(
    genderDirPath.resolve("byEnding.yml"),
    genderStatsByEnding,
    ending => "-" + ending
  )

  val filteredGenderStatsByEnding =
    compactGenderStats(
      genderStatsByEnding,
      (potentiallyImplying, potentiallyImplied) => potentiallyImplied.endsWith(potentiallyImplying),
      1.0
    )

  writeGenderStatsByGroupingAndRelevance(
    genderDirPath.resolve("byEndingAndRelevance.yml"),
    filteredGenderStatsByEnding,
    ending => "-" + ending
  )

  println()
  println()
  println()
}


def getEnding(endingLength: Int, word: String): String = {
  word.substring(
    word.length - endingLength
  )
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


def compactGenderStats(
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
                                statsMap: Map[Int, Map[String, GenderStats]],
                                groupingStringProcessor: String => String
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
              writeGenderStats(
                outputWriter,
                groupingStringProcessor(groupingString),
                stats,
                1
              )
          }

        println()
        println()
      })
  } finally {
    outputWriter.close()
  }
}



def writeGenderStats(
                      outputWriter: PrintWriter,
                      groupingString: String,
                      stats: GenderStats,
                      leadingSpacesCount: Int
                    ): Unit = {
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
                                            statsMap: Map[Int, Map[String, GenderStats]],
                                            groupingStringProcessor: String => String
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
          writeGenderStats(
            outputWriter,
            groupingStringProcessor(groupingString),
            stats,
            0
          )
      }

    println()
    println()

  } finally {
    outputWriter.close()
  }
}


def computeGenderStatsByBeginning(genderDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val genderStatsByBeginning =
    computeGenderStatsByGrouping(
      nouns,
      (beginningLength, nominative) => nominative.substring(0, beginningLength)
    )

  println("== COMPUTING GENDER STATS BY BEGINNING ==")
  println()

  writeGenderStatsByGrouping(
    genderDirPath.resolve("byBeginning.yml"),
    genderStatsByBeginning,
    beginning => beginning + "-"
  )

  val filteredGenderStatsByBeginning =
    compactGenderStats(
      genderStatsByBeginning,
      (potentiallyImplying, potentiallyImplied) => potentiallyImplied.startsWith(potentiallyImplying),
      1.0
    )

  writeGenderStatsByGroupingAndRelevance(
    genderDirPath.resolve("byBeginningAndRelevance.yml"),
    filteredGenderStatsByBeginning,
    beginning => beginning + "-"
  )

  println()
  println()
  println()
}



def computePlurals(outputDirPath: Path, nouns: Iterable[GermanNoun]): Unit = {
  val pluralDirPath =
    outputDirPath.resolve("plural")

  Files.createDirectories(pluralDirPath)

  computePluralsByEndingAndGender(
    pluralDirPath,
    nouns
  )
}



type ReplacedEnding = String

type AddedEnding = String


case class PluralTransform(noun: GermanNoun) extends Ordered[PluralTransform] {
  val (hasApophony, replacedEndingOption, addedEndingOption): (Boolean, Option[ReplacedEnding], Option[AddedEnding]) = {
    val singularCharsList =
      noun.singularNominative.toCharArray.toList

    val pluralCharsList =
      noun.pluralNominative.get.toCharArray.toList

    computePluralTransformElements(singularCharsList, pluralCharsList, apophonyFound = false)
  }

  @tailrec
  private def computePluralTransformElements(singularCharsList: List[Char], pluralCharsList: List[Char], apophonyFound: Boolean): (Boolean, Option[ReplacedEnding], Option[AddedEnding]) = {
    singularCharsList match {
      case Nil =>
        (
          apophonyFound,
          None,
          buildEndingOptionFromList(pluralCharsList)
        )

      case singularHead :: singularTail =>
        val pluralHead :: pluralTail =
          pluralCharsList

        if (singularHead == pluralHead)
          computePluralTransformElements(singularTail, pluralTail, apophonyFound)
        else if (!apophonyFound) {
          val currentHeadHasApophony =
            targetHasApophony(singularHead, pluralHead)

          if (currentHeadHasApophony)
            computePluralTransformElements(singularTail, pluralTail, apophonyFound = true)
          else
            (
              false,
              buildEndingOptionFromList(singularCharsList),
              buildEndingOptionFromList(pluralCharsList)
            )
        } else {
          (
            true,
            buildEndingOptionFromList(singularCharsList),
            buildEndingOptionFromList(pluralCharsList)
          )
        }
    }
  }


  override def compare(that: PluralTransform): Int =
    toString.compareTo(that.toString)


  override def toString: String = {
    val addingLine =
      if (hasApophony)
        "⸚"
      else
        "-"

    addedEndingOption match {
      case Some(addedEnding) =>
        replacedEndingOption match {
          case Some(replacedEnding) =>
            s"-${replacedEnding} ⇒ ${addingLine}${addedEnding}"

          case None =>
            s"${addingLine}${addedEnding}"
        }

      case None =>
        addingLine
    }
  }
}


def buildEndingOptionFromList(source: List[Char]): Option[String] =
  source match {
    case Nil =>
      None

    case _ =>
      Some(new String(source.toArray))
  }


def targetHasApophony(source: Char, target: Char): Boolean =
  (source == 'A' && target == 'Ä') ||
    (source == 'a' && target == 'ä') ||
    (source == 'O' && target == 'Ö') ||
    (source == 'o' && target == 'ö') ||
    (source == 'U' && target == 'Ü') ||
    (source == 'u' && target == 'ü')


case class FrequencyTracker[T](item: T, frequency: Int, universeCount: Int) {
  val percentage: Double =
    frequency * 100.0 / universeCount
}


case class FrequencyCounter[T](items: Iterable[T]) {
  private val universeCount =
    items.size

  val frequencies: List[FrequencyTracker[T]] =
    items
      .groupBy(item => item)
      .map {
        case (item, occurrences) =>
          FrequencyTracker(
            item,
            occurrences.size,
            universeCount
          )
      }
      .toList
      .sortBy(_.frequency)
}


def computePluralsByEndingAndGender(
                                     pluralDirPath: Path,
                                     nouns: Iterable[GermanNoun],
                                     maxEndingLength: Int = 7,
                                     minNounsCountPerRule: Int = 50
                                   ): Unit = {

  println("== COMPUTING PLURAL RULES BY ENDING AND GENDER ==")
  println()

  val outputFilePath = pluralDirPath.resolve("byEndingAndGender.yml")

  val nounsWithPlural =
    nouns
      .filter(_.pluralNominative.nonEmpty)

  val rules =
    Range.inclusive(1, maxEndingLength)
      .flatMap(endingLength => {
        nounsWithPlural
          .filter(
            _.singularNominative.length >= endingLength
          )
          .groupBy(noun => {
            val ending =
              getEnding(endingLength, noun.singularNominative)

            (
              ending,
              noun.gender
            )
          })
          .filter {
            case ((ending, gender), groupedNouns) =>
              groupedNouns.size >= minNounsCountPerRule
          }
          .filter {
            case ((ending, gender), groupedNouns) =>
              Character.isLowerCase(ending(0))
          }
          .map {
            case ((ending, gender), groupedNouns) =>
              (ending, gender) ->
                groupedNouns.map(PluralTransform)
          }
          .map {
            case ((ending, gender), transforms) =>
              (ending, gender) ->
                FrequencyCounter[String](
                  transforms.map(_.toString)
                )
                  .frequencies
                  .reverse
          }
      })


  println(s"Rules ready: ${rules.size}")

  val compactRules =
    rules
      .filter {
        case ((ending, gender), transformFrequencies) =>
          !rules
            .filter {
              case ((rawEnding, _), _) =>
                rawEnding.length < ending.length
            }
            .exists {
              case ((rawEnding, rawGender), rawTransformFrequencies) =>
                ending.endsWith(rawEnding) &&
                  (gender == rawGender) &&
                  (transformFrequencies.map(_.item).toSet == rawTransformFrequencies.map(_.item).toSet)
            }
      }
      .toMap
      .groupBy {
        case ((ending, gender), transformFrequencies) =>
          transformFrequencies.size
      }


  println(s"Compact rules ready: ${compactRules.values.map(_.size).sum}")

  println()

  writePluralByEndingAndGender(
    outputFilePath,
    compactRules
  )

  println()
  println()
  println()
}


def writePluralByEndingAndGender(
                                  outputFilePath: Path,
                                  rulesMap: Map[Int, Map[(String, String), List[FrequencyTracker[String]]]],
                                  minFrequencyPercentage: Double = 1
                                ): Unit = {
  val outputWriter = new PrintWriter(Files.newBufferedWriter(outputFilePath))

  try {
    val sortedRules =
      rulesMap
        .map {
          case (transformsCount, pluralRules) =>
            transformsCount ->
              pluralRules
                .toList
                .map {
                  case ((ending, gender), transformFrequencies) =>
                    (ending, gender) ->
                      transformFrequencies
                        .filter(
                          _.percentage >= minFrequencyPercentage
                        )
                }
                .filter {
                  case ((ending, gender), transformFrequencies) =>
                    transformFrequencies.nonEmpty
                }
                .sortBy {
                  case ((ending, gender), transformFrequencies) =>
                    (
                      100 - transformFrequencies.head.percentage,
                      ending,
                      gender
                    )
                }
        }
        .filter {
          case (transformsCount, pluralRules) =>
            pluralRules.nonEmpty
        }
        .toList
        .sortBy {
          case (transformsCount, pluralRules) =>
            transformsCount
        }


    sortedRules
      .foreach {
        case (transformsCount, pluralRules) =>
          outputWriter.println(s"${transformsCount}:")

          pluralRules
            .foreach {
              case ((ending, gender), transformFrequencies) =>
                outputWriter.println(" - ")
                outputWriter.println(s"   ending: ${ending}")
                outputWriter.println(s"   gender: ${gender}")
                outputWriter.println(s"   totalCount: ${transformFrequencies.head.universeCount}")
                outputWriter.println(s"   maxPercentage: ${transformFrequencies.head.percentage}")
                outputWriter.println(s"   transformFrequencies:")

                transformFrequencies
                  .foreach(transformFrequency => {
                    outputWriter.println(s"    - ")

                    outputWriter.println(s"      transform: '${transformFrequency.item}'")
                    outputWriter.println(s"      count: ${transformFrequency.frequency}")
                    outputWriter.println(f"      percentage: ${transformFrequency.percentage}%.2f")
                  })
            }
      }
  } finally {
    outputWriter.close()
  }
}
