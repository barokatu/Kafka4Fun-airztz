package SpringWebApplication;

import java.io.IOException;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class Application extends SpringBootServletInitializer {// for
																// deployment/local
																// test,
																// //remember to
																// click
																// Source->organize
																// imports
																// before
																// building war,
																// and in the
																// server,
																// remember to
																// remove the
																// previous
																// compiled
																// files
	// public class Application {//only works for local test
	// kill `lsof -i -n -P | grep TCP | grep 9094 | tr -s " " "\n" | sed -n 2p`
	// kill `lsof -i -n -P | grep TCP | grep 9095 | tr -s " " "\n" | sed -n 2p`
	// kill `lsof -i -n -P | grep TCP | grep 4040 | tr -s " " "\n" | sed -n 2p`
	public static void main(String[] args) throws IOException, InterruptedException {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public RestTemplate InitializeRestTemplate() {
		return new RestTemplate();
	}

	@Bean
	public CommandLineRunner InitializeSampleProducer(RestTemplate restTemplate) throws Exception {
		return args -> {
			Demo.runDemo("testInput1", "testOutput1");
			System.exit(0);
		};
	}
}
