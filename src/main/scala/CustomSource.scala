import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSource {

  case class TextOut(data:String )

  def generateRandomStringSource(out: SourceContext[TextOut]) = {
    val lines = Array("how are you", "you are how", " i am fine")
    while (true) {
      val index = Random.nextInt(3)
      Thread.sleep(200)
      out.collect(TextOut(lines(index)))
    }
  }


  def main(args: Array[String]) {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    streamEnv.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    val sink = StreamingFileSink.forBulkFormat(new Path("file:///tmp/test2"),
      ParquetAvroWriters.forReflectRecord(classOf[TextOut])).build()

    val customSource = streamEnv.addSource(generateRandomStringSource _)

    customSource.print()

    customSource.addSink(sink)




    streamEnv.execute()

  }

}
