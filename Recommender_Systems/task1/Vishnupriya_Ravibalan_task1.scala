import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.functions._
import java.io._


object Vishnupriya_Ravibalan_task1
{	
	def main(args: Array[String])
	{

	    val sc = new SparkContext()
	    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	    import sqlContext.implicits._
	    //PROCESS DATA FROM RATINGS.CSV
	    val t1 = System.nanoTime
		val Ratings_file = args(0)
	    val Ratings_rdd = sc.textFile(Ratings_file);
	    //drop the header
	    val Full_rdd = Ratings_rdd.mapPartitionsWithIndex { (i, iter) => if (i == 0) iter.drop(1) else iter }
		val Data_rdd = Full_rdd.map(x=>x.split(",")).map {
		case Array(uid, movieid, rating, timestamp) => Rating(uid.toInt, movieid.toInt, rating.toDouble)
		}

		//PROCESS DATA FROM TESTINGSMALL.CSV
		val Testing_small_file = args(1)
	    val TestRatings_rdd = sc.textFile(Testing_small_file);
	   	//drop the header
	    val Test_rdd = TestRatings_rdd.mapPartitionsWithIndex { (i, iter) => if (i == 0) iter.drop(1) else iter }
		val TestFile = Test_rdd.map(x=>x.split(",")).map {
		case Array(uid, movieid) => Rating(uid.toInt, movieid.toInt, 0)
		}

		//REMOVE TEST DATA FROM FULL DATA TO GET TRAINING DATA
		val RDD_RF = Data_rdd.map(x => (x.user,x.product)->x.rating)
		val RDD_TF = TestFile.map(x => (x.user,x.product)->x.rating)
		val Test = RDD_RF.join(RDD_TF)
		val Train = RDD_RF.subtractByKey(Test)

		//TRAIN THE RECOMMENDATION MODEL	
		val New_Train = Train.map(x => Rating(x._1._1,x._1._2,x._2))	
		val rank = 10
		val numIterations = 12
		val model = ALS.train(New_Train, rank, numIterations, 0.1)

		//FIND PREDICTIONS
		val New_Test = Test.map(x => ((x._1._1,x._1._2),x._2._1))
		val S_Test = New_Test.map(x =>(x._1._1,x._1._2))
		val predictions = model.predict(S_Test).map{ case Rating(x, y, z) => if (z<0) ((x, y), 0.00000) else if (z>5) ((x, y), 5.00000) else ((x, y), z)}

    	//FIND USER AVERAGES
    	var user = Train.map { case((x, y), z) =>(x.toInt, z.toDouble)}
	    val temp1 = user.groupByKey.mapValues(_.toList)
	    val user_average = temp1.map { case(x, y) =>(x, y.sum/y.length)}


		//FIND MISSING VALUES
		val EmptyTest = Test_rdd.map(x=>x.split(",")).map {case Array(uid, movieid) => Rating(uid.toInt, movieid.toInt, 0.00000)}.map(x => (x.user,x.product)->x.rating)
		val temp = EmptyTest.subtractByKey(predictions).map { case((x, y), z) =>(x, y)}
		val missingvals = temp.join(user_average).map { case(x, (y, z)) => ((x, y), z)}
		val finalpredic = predictions.union(missingvals)


		val Test_Pred = New_Test.join(finalpredic)


		//FIND RMSE
		val MSE = Test_Pred.map { case ((user, product), (r1, r2)) =>
		  val err = (r1 - r2)
		  err * err
		}.mean()
		val Difference = Test_Pred.map { case ((user, product), (r1, r2)) =>
		  val err = (r1 - r2)
		  if (err < 1) ">=0 and <1"
		  else if (err < 2) ">=1 and <2"
		  else if (err < 3) ">=2 and <3"
		  else if (err < 4) ">=3 and <4"
		  else ">=4"
		}.toDF("1")

		// PRINT TO OUTPUT FILE
		val answer = finalpredic.map{ case((x, y), z) => (x, y, z)}
	    val op = new PrintWriter(new File("Vishnupriya_Ravibalan_result_task1.txt"))
	    op.write("userID,movieID,rating\n")
	    val output = answer.sortBy(r => (r._1, r._2, r._3)).collect.toList
	    for(line <- output){
	      op.write(line._1+","+line._2+","+line._3+"\n")
	    }
	    op.close()

		val Error = Difference.groupBy("1").agg(count("1")).orderBy("1").rdd
		Error.map(x => "%s: %s".format(x(0),x(1))).sortBy(_(0)).foreach(println)

		println("RMSE = " + MSE)
		
		val duration = (System.nanoTime - t1)/ 1e9d
		print("The total time taken for execution is ")
		print(duration)
		print(" seconds\n")
	}
}
