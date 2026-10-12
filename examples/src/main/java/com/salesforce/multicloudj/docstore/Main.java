package com.salesforce.multicloudj.docstore;

import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import com.salesforce.multicloudj.examples.AppConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class demonstrating DocStore operations across different cloud providers. This example shows
 * how to use the multicloudj library for document storage operations.
 *
 * <p>Usage: {@code java -cp examples/target/multicloudj-examples-<version>.jar
 * com.salesforce.multicloudj.docstore.Main [provider-id] [table-name]}. The provider id defaults to
 * {@code docstore.provider}, else the global {@code provider}; the table name defaults to {@code
 * docstore.table}; the region comes from the global {@code region} (see {@code
 * examples.properties}).
 */
public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  // Constants
  private static final String KEY_PUBLISHER = "publisher";
  private static final String KEY_TITLE = "title";
  private static final String REVISION_FIELD = "docRevision";

  // Demo settings
  private static final int STATUS_BAR_WIDTH = 50;
  private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  // Sample data
  private static final Person SAMPLE_PERSON = new Person("Zoe", "Ford", 22);
  private static final Book SAMPLE_BOOK =
      new Book(
          "YellowBook",
          SAMPLE_PERSON,
          "WA",
          3.99f,
          new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
          null);

  // Runtime configuration
  private final String provider;
  private final String tableName;
  private final String region;

  /** Data model representing a person. */
  @AllArgsConstructor
  @Data
  @NoArgsConstructor
  static class Person {
    private String firstName;
    private String lastName;
    private int age;
  }

  /** Data model representing a book with nested person object. */
  @AllArgsConstructor
  @Data
  @NoArgsConstructor
  static class Book {
    private String title;
    private Person author;
    private String publisher;
    private float price;
    private Map<String, Integer> tableOfContents;
    private Object docRevision;
  }

  /** Constructor that accepts provider, table name and region configuration. */
  public Main(String provider, String tableName, String region) {
    this.provider = provider;
    this.tableName = tableName;
    this.region = region;
  }

  public static void main(String[] args) {
    // Parse command line arguments, falling back to configuration
    String provider = parseProvider(args);
    String tableName = parseTableName(args);
    String region = AppConfig.get("region");

    // Display welcome banner
    printWelcomeBanner();

    // Display configuration
    printConfiguration(provider, tableName, region);

    Main main = new Main(provider, tableName, region);
    main.runDemo();

    // Display completion banner
    printCompletionBanner();

    // Close reader
    try {
      reader.close();
    } catch (IOException e) {
      // Ignore
    }
  }

  /** Print a welcome banner for the demo. */
  private static void printWelcomeBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║                    🚀 MultiCloudJ DocStore Demo 🚀                        ║");
    logger.info("║                    Cross-Cloud Document Storage                            ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
  }

  /** Print the configuration being used. */
  private static void printConfiguration(String provider, String tableName, String region) {
    logger.info("📋 Configuration:");
    logger.info("   Provider: {}", provider);
    logger.info("   Table/Collection: {}", tableName);
    logger.info("   Region: {}", region);
    logger.info("");
    waitForEnter("Press Enter to start the demo...");
  }

  /** Print a completion banner. */
  private static void printCompletionBanner() {
    logger.info("");
    logger.info("╔══════════════════════════════════════════════════════════════════════════════╗");
    logger.info("║                    ✅ Demo Completed Successfully! ✅                       ║");
    logger.info("║                    Thanks for trying MultiCloudJ!                          ║");
    logger.info("╚══════════════════════════════════════════════════════════════════════════════╝");
    logger.info("");
    waitForEnter("Press Enter to exit...");
  }

  /** Parse provider from command line arguments, falling back to configuration. */
  private static String parseProvider(String[] args) {
    if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
      return args[0].trim();
    }
    // Never hardcode the provider id; resolve it from configuration (see examples.properties)
    return AppConfig.provider("docstore");
  }

  /** Parse table name from command line arguments, falling back to configuration. */
  private static String parseTableName(String[] args) {
    if (args.length > 1 && args[1] != null && !args[1].trim().isEmpty()) {
      return args[1].trim();
    }
    return AppConfig.get("docstore.table");
  }

  /** Wait for user to press Enter key. */
  private static void waitForEnter(String message) {
    logger.info(message);
    try {
      reader.readLine();
    } catch (IOException e) {
      // If there's an error reading input, just continue
      logger.info("(Continuing automatically...)");
    }
  }

  /** Display a simple progress indicator. */
  private static void showProgress(String message, int current, int total) {
    logger.info("🔄 {} ({}/{})", message, current, total);
  }

  /** Display a success message with emoji and wait for user input. */
  private static void showSuccess(String message) {
    logger.info("✅ {}", message);
  }

  /** Display an info message with emoji and wait for user input. */
  private static void showInfo(String message) {
    logger.info("ℹ️  {}", message);
  }

  /** Display a section header and wait for user input. */
  private static void showSectionHeader(String title) {
    logger.info("");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("📚 {}", title);
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("");
  }

  /** Display a section header with pause for major transitions. */
  private static void showSectionHeaderWithPause(String title) {
    logger.info("");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("📚 {}", title);
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("");
    waitForEnter("Press Enter to start this section...");
  }

  /** Display a success message without waiting (for status bar completion). */
  private static void showSuccessNoWait(String message) {
    logger.info("✅ {}", message);
  }

  /** Main demo method that orchestrates all the DocStore operations. */
  private void runDemo() {
    showInfo("Starting DocStore demo with provider: " + provider);

    // Initialize client
    showInfo("Initializing DocStore client...");
    DocStoreClient client = initializeClient();
    showSuccess("Client initialized successfully!");

    // Run different operation demos
    cleanupExistingData(client);
    demonstrateCreateOperations(client);
    demonstrateReplaceOperations(client);
    demonstrateQueryOperations(client);
    demonstrateDeleteOperations(client);

    showSuccess("DocStore demo completed successfully!");
  }

  /** Initialize the DocStore client with appropriate configuration. */
  private DocStoreClient initializeClient() {
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName(tableName)
            .withPartitionKey(KEY_TITLE)
            .withSortKey(KEY_PUBLISHER)
            .withRevisionField(REVISION_FIELD)
            .withAllowScans(true)
            .build();

    return DocStoreClient.builder(provider)
        .withRegion(region)
        .withCollectionOptions(collectionOptions)
        .build();
  }

  /** Clean up any existing data in the collection. */
  private void cleanupExistingData(DocStoreClient client) {
    showSectionHeader("Data Cleanup");
    showInfo("Cleaning up existing data...");

    DocumentIterator iterator = client.query().get();
    ActionList actionList = client.getActions();

    int count = 0;
    while (iterator.hasNext()) {
      Map<?, ?> emptyMap = new HashMap<>();
      Document document = new Document(emptyMap);
      iterator.next(document);
      actionList.delete(document);
      count++;
      showProgress("Deleting existing documents", count, count);
    }

    if (count > 0) {
      actionList.run();
      showSuccessNoWait("Deleted " + count + " existing documents");
    } else {
      showSuccessNoWait("No existing documents found");
    }
  }

  /** Demonstrate CREATE operations. */
  private void demonstrateCreateOperations(DocStoreClient client) {
    showSectionHeaderWithPause("CREATE Operations");

    // Create a single book
    showInfo("Creating 1 book: " + SAMPLE_BOOK.getTitle());
    client.create(new Document(SAMPLE_BOOK));
    showSuccess("Created book: " + SAMPLE_BOOK.getTitle());

    // Create multiple books with atomic writes
    showInfo("Creating multiple books with atomic writes...");
    client
        .getActions()
        .enableAtomicWrites()
        .create(new Document(new Book("RedBook", SAMPLE_PERSON, "CA", 4.99f, null, null)))
        .create(new Document(new Book("GreenBook", SAMPLE_PERSON, "NY", 5.99f, null, null)))
        .create(new Document(new Book("BlueBook", SAMPLE_PERSON, "TX", 6.99f, null, null)))
        .run();
    showSuccess("Created 3 books with atomic writes");

    // Create with specific field retrieval
    showInfo("Creating book and retrieving specific field (author.lastName)...");
    Book bookWithActions = new Book("PurpleBook", SAMPLE_PERSON, "FL", 7.99f, null, null);
    client
        .getActions()
        .create(new Document(bookWithActions))
        .get(new Document(bookWithActions), "author.lastName")
        .run();
    showSuccess(
        "Created 1 another book and retrieved author: "
            + bookWithActions.getAuthor().getLastName());

    waitForEnter("Press Enter to continue to next section...");
  }

  /** Demonstrate REPLACE operations. */
  private void demonstrateReplaceOperations(DocStoreClient client) {
    showSectionHeaderWithPause("REPLACE Operations");

    // Replace a book's price
    showInfo("Replacing book price...");
    Book bookToReplace = new Book("YellowBook", null, "WA", 0, null, null);
    client.get(new Document(bookToReplace)); // First get the current book
    float oldPrice = bookToReplace.getPrice();
    bookToReplace.setPrice(9.99f);
    client.getActions().replace(new Document(bookToReplace)).get(new Document(bookToReplace)).run();
    showSuccess("Replaced book price from $" + oldPrice + " to $" + bookToReplace.getPrice());

    // Replace multiple fields atomically
    showInfo("Replacing multiple fields atomically...");
    Book bookForMultiReplace = new Book("RedBook", null, "CA", 0, null, null);
    client.get(new Document(bookForMultiReplace));
    bookForMultiReplace.setPrice(12.99f);
    client
        .getActions()
        .replace(new Document(bookForMultiReplace))
        .get(new Document(bookForMultiReplace))
        .run();
    showSuccess(
        "Replaced multiple fields: price=$"
            + bookForMultiReplace.getPrice()
            + ", publisher="
            + bookForMultiReplace.getPublisher());

    // Replace with conditional logic
    showInfo("Replacing book with conditional logic...");
    Book conditionalBook = new Book("GreenBook", null, "NY", 0, null, null);
    client.get(new Document(conditionalBook));
    if (conditionalBook.getPrice() < 10.0f) {
      conditionalBook.setPrice(conditionalBook.getPrice() * 1.5f);
      client
          .getActions()
          .replace(new Document(conditionalBook))
          .get(new Document(conditionalBook))
          .run();
      showSuccess("Applied 50% price increase: $" + conditionalBook.getPrice());
    }

    waitForEnter("Press Enter to continue to next section...");
  }

  /** Demonstrate QUERY operations. */
  private void demonstrateQueryOperations(DocStoreClient client) {
    showSectionHeaderWithPause("QUERY Operations");

    // Simple query by title
    showInfo("Querying books by title (YellowBook)...");
    DocumentIterator titleQuery =
        client.query().where(KEY_TITLE, FilterOperation.EQUAL, "YellowBook").get();

    Book queriedBook = new Book();
    Document document = new Document(queriedBook);
    int count = 0;
    while (titleQuery.hasNext()) {
      titleQuery.next(document);
      count++;
      showSuccess(
          "Found book "
              + count
              + ": "
              + queriedBook.getTitle()
              + " from "
              + queriedBook.getPublisher());
    }

    // Query with multiple conditions
    showInfo("Querying with multiple conditions (price < $10, publisher starts with 'C')...");
    DocumentIterator multiQuery =
        client
            .query()
            .where("price", FilterOperation.LESS_THAN, 10)
            .where(KEY_PUBLISHER, FilterOperation.GREATER_THAN, "C")
            .limit(3)
            .get();

    count = 0;
    while (multiQuery.hasNext()) {
      multiQuery.next(document);
      count++;
      showSuccess(
          "Multi-condition result "
              + count
              + ": "
              + queriedBook.getTitle()
              + " ($"
              + queriedBook.getPrice()
              + ") from "
              + queriedBook.getPublisher());
    }

    // Query with offset and pagination
    showInfo("Querying with offset and pagination...");
    DocumentIterator paginatedQuery =
        client.query().where("price", FilterOperation.GREATER_THAN, 0).limit(2).offset(0).get();

    count = 0;
    while (paginatedQuery.hasNext()) {
      paginatedQuery.next(document);
      count++;
      showSuccess(
          "Page 1, Item "
              + count
              + ": "
              + queriedBook.getTitle()
              + " ($"
              + queriedBook.getPrice()
              + ")");
    }

    // Continue with pagination token
    PaginationToken token = paginatedQuery.getPaginationToken();
    if (token != null) {
      showInfo("Continuing with pagination token...");
      DocumentIterator nextPage =
          client
              .query()
              .where("price", FilterOperation.GREATER_THAN, 0)
              .limit(2)
              .paginationToken(token)
              .get();

      count = 0;
      while (nextPage.hasNext()) {
        nextPage.next(document);
        count++;
        showSuccess(
            "Page 2, Item "
                + count
                + ": "
                + queriedBook.getTitle()
                + " ($"
                + queriedBook.getPrice()
                + ")");
      }
    }

    // Scan all documents
    showInfo("Scanning all documents...");
    DocumentIterator scanIterator = client.query().get();
    int totalCount = 0;
    while (scanIterator.hasNext()) {
      scanIterator.next(document);
      totalCount++;
      showProgress("Scanned documents", totalCount, totalCount);
    }
    showSuccessNoWait("Total documents in collection: " + totalCount);

    waitForEnter("Press Enter to continue to next section...");
  }

  /** Demonstrate DELETE operations. */
  private void demonstrateDeleteOperations(DocStoreClient client) {
    showSectionHeaderWithPause("DELETE Operations");

    // Delete a specific book
    showInfo("Deleting a specific book (PurpleBook)...");
    Book bookToDelete = new Book("PurpleBook", null, "FL", 0, null, null);
    client.getActions().delete(new Document(bookToDelete)).run();
    showSuccess("Deleted book: PurpleBook");

    // Delete multiple books atomically
    showInfo("Deleting multiple books atomically...");
    client
        .getActions()
        .delete(new Document(new Book("RedBook", null, "OR", 0, null, null)))
        .delete(new Document(new Book("GreenBook", null, "NY", 0, null, null)))
        .enableAtomicWrites()
        .delete(new Document(new Book("BlueBook", null, "TX", 0, null, null)))
        .run();
    showSuccess("Deleted 3 books with atomic operations");

    // Delete with query (delete all books with price > $10)
    showInfo("Deleting books with price > $10...");
    DocumentIterator expensiveBooks =
        client.query().where("price", FilterOperation.GREATER_THAN, 10).get();

    ActionList deleteActionList = client.getActions();
    int deleteCount = 0;
    while (expensiveBooks.hasNext()) {
      Book expensiveBook = new Book();
      expensiveBooks.next(new Document(expensiveBook));
      deleteActionList.delete(new Document(expensiveBook));
      deleteCount++;
      showProgress("Marked for deletion", deleteCount, deleteCount);
    }

    if (deleteCount > 0) {
      deleteActionList.run();
      showSuccess("Deleted " + deleteCount + " expensive books");
    } else {
      showSuccess("No expensive books found to delete");
    }

    // Show remaining documents
    showInfo("Showing remaining documents...");
    DocumentIterator remainingDocs = client.query().get();
    int remainingCount = 0;
    while (remainingDocs.hasNext()) {
      Book remainingBook = new Book();
      remainingDocs.next(new Document(remainingBook));
      remainingCount++;
      showSuccess(
          "Remaining book "
              + remainingCount
              + ": "
              + remainingBook.getTitle()
              + " ($"
              + remainingBook.getPrice()
              + ")");
    }

    showSuccess("Delete operations completed. Remaining documents: " + remainingCount);
  }
}
