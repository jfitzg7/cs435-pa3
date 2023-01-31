import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object IdealizedPageRank {
    def pageRank(lines: RDD[String], sortedTitles: String, sc:SparkContext) : RDD[(String,Double)] = {
        val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
        val totalPages = links.count()
        var ranks = links.mapValues(v => 1.0 / totalPages)

        for (i <- 1 to 25) {
            val tempRanks = links.join(ranks).values.flatMap {
                case (urls, rank) =>
                val outgoingLinks = urls.split(" ")
                outgoingLinks.map(url => (url, rank / outgoingLinks.length))
            }
            ranks = tempRanks.reduceByKey(_ + _)
        }

        val titles = sc.textFile(sortedTitles).zipWithIndex().mapValues(x=>(x+1).toString).map(_.swap)
        val output = ranks.join(titles).values.map(_.swap).sortBy(_._2, false)
        return output.cache()
    }
}

object SurfingFilter {
    def filter(lines :RDD[String], sortedTitles: String, sc:SparkContext) : RDD[String] = {
        val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))

        // Read in the article titles, map them with their ID,
        // then reduce them to just the ones with "surfing" in the title.
        val titles = sc.textFile(sortedTitles).zipWithIndex().mapValues(x=>(x+1).toString).map(_.swap)
        val surfingTitles = titles.filter{case (id, title) => title.toLowerCase().contains("surfing")}

        // Join the surfing IDs with the links table (to get just the links for just the surifng articles),
        // then remove the titles.
        val surfingLinks = surfingTitles.join(links).map{case (id, (title, links)) => (id, links)}

        // Output the surfing links in the same format as it initally was.
        val output = surfingLinks.map{case (source, destinations) => source + ": " + destinations}
        return output.cache()
    }
}

object TaxationPageRank {
    def pageRank(lines: RDD[String], sortedTitles:String, sc:SparkContext) : RDD[(String,Double)] = {
        val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
        val totalPages = links.count()
        var ranks = links.mapValues(v => 1.0 / totalPages)
        val taxationConstants = links.mapValues(v => 0.15 / totalPages)

        for (i <- 1 to 25) {
            val tempRanks = links.join(ranks).values.flatMap {
                case (urls, rank) =>
                val outgoingLinks = urls.split(" ")
                outgoingLinks.map(url => (url, (rank / outgoingLinks.length) * 0.85))
            }
            ranks = tempRanks.union(taxationConstants).reduceByKey(_ + _)
        }

        val titles = sc.textFile(sortedTitles).zipWithIndex().mapValues(x=>(x+1).toString).map(_.swap)
        val output = ranks.join(titles).values.map(_.swap).sortBy(_._2, false)
        return output.cache()
    }
}

object ModifyLinks {
    def modify(lines: RDD[String], sortedTitles:String, sc:SparkContext) : RDD[String] = {
        var RMNP: String = "4290745"
        val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
        var surf_lines = SurfingFilter.filter(lines, sortedTitles, sc).cache()
        var surf_links = surf_lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
        var filtered_links= links.subtractByKey(surf_links)
        var alteredSurfLinks = surf_links.mapValues(l => l + " " + RMNP)
        var union = filtered_links.union(alteredSurfLinks)
        val output = union.map{case (source, destinations) => source + ": " + destinations}
        return output.cache()
    }
}

object SurfingPageRank{
    def main(args: Array[String]){
        val sortedLinks = args(1)
        val sortedTitles = args(2)
        val outputDir = args(3)
        val profile = args(4)

        val sc = SparkSession.builder().master(args(0)).getOrCreate().sparkContext
        val lines = sc.textFile(sortedLinks)

        profile match {
            case "IdealizedPageRank" => {
                val pageRank = IdealizedPageRank.pageRank(lines, sortedTitles, sc)
                sc.parallelize(pageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_Idealized_Page_Rank")
            }
            case "TaxationPageRank" => {
                val pageRank = TaxationPageRank.pageRank(lines, sortedTitles, sc)
                sc.parallelize(pageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_Taxation_Page_Rank")
            }
            case "SurfSubgraph" => {
                val surfSubGraph = SurfingFilter.filter(lines, sortedTitles, sc)
                surfSubGraph.coalesce(1).saveAsTextFile(outputDir + "_Surfing_Subgraph")
            }
            case "ModifySurfGraph" => {
                val modifiedLinks = ModifyLinks.modify(lines, sortedTitles, sc)
                modifiedLinks.coalesce(1).saveAsTextFile(outputDir + "_Modified_Graph")
                val modifiedSurfSubgraph = SurfingFilter.filter(modifiedLinks, sortedTitles, sc)
                modifiedSurfSubgraph.coalesce(1).saveAsTextFile(outputDir + "_Modified_Surf_Subgraph")
            }
            case "TPageRankOnSubGraph" => {
                val surfSubGraph = SurfingFilter.filter(lines, sortedTitles, sc)
                val pageRank = TaxationPageRank.pageRank(surfSubGraph, sortedTitles, sc)
                sc.parallelize(pageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_TR_On_Subgraph")
            }
            case "TPageRankOnModifiedSubGraph" => {
                val modifiedLinks = ModifyLinks.modify(lines, sortedTitles, sc)
                val modifiedSurfSubgraph = SurfingFilter.filter(modifiedLinks, sortedTitles, sc)
                val pageRank = TaxationPageRank.pageRank(modifiedSurfSubgraph, sortedTitles, sc)
                sc.parallelize(pageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_TR_On_Modified_Subgraph")
            }
            case "runAll" => {
                //val idealizedPageRank = IdealizedPageRank.pageRank(lines, sortedTitles, sc)
                //val taxationPageRank = TaxationPageRank.pageRank(lines, sortedTitles, sc)
                val surfSubGraph = SurfingFilter.filter(lines, sortedTitles, sc)
                val subGraphPageRank = TaxationPageRank.pageRank(surfSubGraph, sortedTitles, sc)
                sc.parallelize(subGraphPageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_Subgraph_PageRank")
                val modifiedLinks = ModifyLinks.modify(lines, sortedTitles, sc)
                val modifiedSurfSubgraph = SurfingFilter.filter(modifiedLinks, sortedTitles, sc)
                val modifiedSubGraphPageRank = TaxationPageRank.pageRank(modifiedSurfSubgraph, sortedTitles, sc)
                sc.parallelize(modifiedSubGraphPageRank.take(10)).coalesce(1).saveAsTextFile(outputDir + "_Modified_Subgraph_PageRank") 
            }
            case _ => {
                System.exit(0)
            }
        }
    }
}
