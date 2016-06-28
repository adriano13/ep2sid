package EPSID.Weather;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class App 
{
	 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
		  private final static IntWritable one = new IntWritable(1);
		  private Text word = new Text();
		  
		  @Override
		  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		         String line = value.toString();
		         System.out.println(line);
		         StringTokenizer tokenizer = new StringTokenizer(line);
		        
		         String linha = value.toString();
	             String ano = linha.substring(14, 18).trim();
	             String mes = linha.substring(19, 20).trim();
	             String dia = linha.substring(21, 22).trim();
	             System.out.println("Ano: "+ano);
	             System.out.println("Mes: "+mes);
	             System.out.println("Dia: "+dia);
	             
	             String KeyTemp = dia+"/"+mes+"/"+ano;
	             
	              String temp= "";
	              String DEWP = "";
	              String SLP = "";
	              String STP = "";
	              String VISIB = "";
	              
	   
	              temp = linha.substring(25, 30);
	              System.out.println("temp: "+temp);
	              DEWP = linha.substring(36, 41);
	              System.out.println("DEWP: "+DEWP);
	              SLP = linha.substring(46, 52);
	              System.out.println("SLP: "+SLP);
	              STP = linha.substring(57, 63);
	              System.out.println("STP: "+STP);
	              VISIB = linha.substring(69, 73);
	              System.out.println("VISIB: "+VISIB);
	              String content = temp+"/"+DEWP+"/"+SLP+"/"+STP+"/"+VISIB;
	             
//	             while (tokenizer.hasMoreTokens()) {
//		               word.set(tokenizer.nextToken());
	               context.write(new Text(KeyTemp),  new FloatWritable(Float.parseFloat(temp)));
//		         }
		      }		  
		    } 
		           
		   public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		   
			   public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
					   	throws IOException, InterruptedException {
//				   float sum = 0;
//		           for (FloatWritable val : values) {
//		               sum += val.get();
//		          }
		           context.write(key, new FloatWritable(1));
		       }
		    }
		           
		    public static void main(String[] args) throws Exception {
		       Configuration conf = new Configuration();
		           
		           Job job = new Job(conf, "nomeJob");
		       
		           job.setOutputKeyClass(Text.class);
		           job.setOutputValueClass(FloatWritable.class);
		           
		           job.setMapperClass(Map.class);
		           job.setReducerClass(Reduce.class);
		           
		           job.setInputFormatClass(TextInputFormat.class);
		           job.setOutputFormatClass(TextOutputFormat.class);
		           
		           FileInputFormat.addInputPath(job, new Path(args[0]));
		           FileOutputFormat.setOutputPath(job, new Path(args[1]));
		           
		           job.waitForCompletion(true);
		    }
		   }
