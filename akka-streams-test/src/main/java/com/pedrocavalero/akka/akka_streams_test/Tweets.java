package com.pedrocavalero.akka.akka_streams_test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.pedrocavalero.akka.akka_streams_test.Tweets.Author;
import com.pedrocavalero.akka.akka_streams_test.Tweets.Hashtag;
import com.pedrocavalero.akka.akka_streams_test.Tweets.Tweet;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.Materializer;
import akka.stream.SinkShape;
import akka.stream.ThrottleMode;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.concurrent.duration.Duration;


public class Tweets {

	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("reactive-tweets");
		final Materializer mat = ActorMaterializer.create(system);
		
		Source<Tweet, NotUsed> tweets = Source.repeat(
				new Tweet(new Author("Pedro"), 1, "Ola #akka")).throttle(1, Duration.create(1, TimeUnit.SECONDS), 1, ThrottleMode.shaping());
//		final Source<Author, NotUsed> authors =
//				  tweets
//				    .filter(t -> t.hashtags().contains(AKKA))
//				    .map(t -> t.author);
		//authors.runWith(Sink.foreach(a -> System.out.println(a)), mat);
		//authors.runForeach(a -> System.out.println(a.handle), mat);
		
		Sink<Author,CompletionStage<Done>> writeAuthors=Sink.foreach(a -> System.out.println(a));
		Sink<Hashtag,CompletionStage<Done>> writeHashtags=Sink.foreach(a -> System.out.println(a));
		RunnableGraph.fromGraph(GraphDSL.create(b -> {
		  final UniformFanOutShape<Tweet, Tweet> bcast = b.add(Broadcast.create(2));
		  final FlowShape<Tweet, Author> toAuthor =
			  b.add(Flow.of(Tweet.class).map(t -> t.author));
		  final FlowShape<Tweet, Hashtag> toTags =
		      b.add(Flow.of(Tweet.class).mapConcat(t -> new ArrayList<Hashtag>(t.hashtags())));
		  final SinkShape<Author> authors = b.add(writeAuthors);
		  final SinkShape<Hashtag> hashtags = b.add(writeHashtags);

		  b.from(b.add(tweets)).viaFanOut(bcast).via(toAuthor).to(authors);
		                             b.from(bcast).via(toTags).to(hashtags);
		  return ClosedShape.getInstance();
		})).run(mat);
	}

	public static class Author {
		  public final String handle;

		  public Author(String handle) {
		    this.handle = handle;
		  }

		@Override
		public String toString() {
			return "Author [handle=" + handle + "]";
		}

		  // ...
		  
		}

		public static class Hashtag {
		  public final String name;

		  public Hashtag(String name) {
		    this.name = name;
		  }

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Hashtag other = (Hashtag) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Hashtag [name=" + name + "]";
		}

		  
		  // ...
		}

		public static class Tweet {
		  public final Author author;
		  public final long timestamp;
		  public final String body;

		  public Tweet(Author author, long timestamp, String body) {
		    this.author = author;
		    this.timestamp = timestamp;
		    this.body = body;
		  }

		  public Set<Hashtag> hashtags() {
		    return Arrays.asList(body.split(" ")).stream()
		      .filter(a -> a.startsWith("#"))
		      .map(a -> new Hashtag(a))
		      .collect(Collectors.toSet());
		  }

		  // ...
		}

		public static final Hashtag AKKA = new Hashtag("#akka");

}
