package io.javabrains.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import connection.DataStaxAstraProperties;
import io.javabrains.betterreadsdataloader.author.Author;
import io.javabrains.betterreadsdataloader.author.AuthorRepository;
import io.javabrains.betterreadsdataloader.book.Book;
import io.javabrains.betterreadsdataloader.book.BookRepository;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpData;

    @Value("${datadump.location.works}")
    private String worksDumpData;

    private void initAuthors() {
        Path path = Paths.get(authorDumpData);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    
                    // Construct Author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    System.out.println(author.getName() + "....");    
                    // Persist using Repository
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void initBooks() {
        Path path = Paths.get(worksDumpData);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        try (Stream<String> lines = Files.lines(path)) {
            lines.limit(50).forEach(line -> {
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    
                    // Construct Author object
                    Book book = new Book();
                    book.setId(jsonObject.optString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title")); 
                    
                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if (descriptionObj != null) {
                        book.setDescription(descriptionObj.optString("value"));
                    }

                    JSONObject publishedDate = jsonObject.optJSONObject("created");
                    if (publishedDate != null) {
                        String dateStr = publishedDate.optString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
                    }

                    JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
                    if (coversJSONArr != null) {
                        List<String> covers = new ArrayList<>();
                        for(int i = 0; i < coversJSONArr.length(); i++) {
                            covers.add(coversJSONArr.getString(i));
                        }

                        book.setCoverIds(covers);
                    }

                    JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
                    if (authorsJSONArr != null) {
                        //Get Author's Ids
                        List<String> authorIds = new ArrayList<String>();
                        for(int i = 0; i < authorsJSONArr.length(); i++) {
                            String authorId = authorsJSONArr.getJSONObject(i)
                            .getJSONObject("author").getString("key").replace("/authors/", "");
                            
                            authorIds.add(authorId);
                        }

                        book.setAuthorId(authorIds);

                        //Get Author's Names
                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                            .map(optionalAuthor -> {
                                if (!optionalAuthor.isPresent()) return "Unknow Author";
                                return optionalAuthor.get().getName();
                            }).collect(Collectors.toList());
                        

                        book.setAuthorName(authorNames);
                        
                    }

                    System.out.println(book.getName() + "....");    
                    // Persist using Repository
                    bookRepository.save(book);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

    @PostConstruct
    public void start() {
        /*Author author= new Author();
        author.setId("id");
        author.setName("name");
        author.setPersonalName("personal name");
        authorRepository.save(author);*/
        //initAuthors();
        initBooks();
    }
    
	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
