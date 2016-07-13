package com.outbrain.aletheia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.*;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.configuration.endpoint.EndPointTemplate;
import com.outbrain.aletheia.configuration.endpoint.InMemoryEndPointTemplate;
import com.outbrain.aletheia.configuration.routing.ExtendedRoutingInfo;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.configuration.routing.RoutingInfo;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class AletheiaConfig {

  private static final Logger logger = LoggerFactory.getLogger(AletheiaConfig.class);

  public static final String BREADCRUMBS = "breadcrumbs";

  public static final String ENDPOINTS_CONFIG_PATH = "aletheia.endpoints.config.path";
  public static final String ROUTING_CONFIG_PATH = "aletheia.routing.config.path";
  public static final String ENDPOINT_GROUPS_CONFIG_PATH = "aletheia.endpoint.groups.config.path";
  public static final String SERDES_CONFIG_PATH = "aletheia.serdes.config.path";

  public static final String SERDES = "aletheia.serdes.config";
  public static final String ENDPOINT_GROUPS = "aletheia.endpoint.groups.config";
  public static final String ROUTING = "aletheia.routing.config";
  public static final String ENDPOINTS = "aletheia.endpoints.config";

  public static final String VERIFY_ROUTING = "aletheia.verify.routing";
  public static final String MULTIPLE_CONFIGURATIONS_PATH = "aletheia.multiple.config.path";
  public static final String ENDPOINT_GROUPS_EXTENSION = "aletheia.multiple.config.endpoint.groups.extension";
  public static final String ENDPOINTS_EXTENSION = "aletheia.multiple.config.endpoints.extension";
  public static final String ROUTING_EXTENSION = "aletheia.multiple.config.routing.extension";
  public static final String SERDES_EXTENSION = "aletheia.multiple.config.serdes.extension";

  private static final String CONSUME_SECTION = "consume";
  private static final String PRODUCE_SECTION = "produce";

  private final ObjectMapper objectMapper;
  private final String endPointsConfig;
  private final String routingConfig;
  private final String endPointGroupsConfig;
  private final String serDeConfig;
  private final Properties properties;

  private static final ObjectMapper simpleObjectMapper = new ObjectMapper();
  private static final Map<String, Class<? extends EndPointTemplate>> customEndPointTemplates = new HashMap<>();


  public AletheiaConfig(final Properties properties) {
    this(properties,
         getPropertyOrReadConfig(ENDPOINTS, ENDPOINTS_CONFIG_PATH, ENDPOINTS_EXTENSION, properties),
         getPropertyOrReadConfig(ROUTING, ROUTING_CONFIG_PATH, ROUTING_EXTENSION, properties),
         getPropertyOrReadConfig(ENDPOINT_GROUPS, ENDPOINT_GROUPS_CONFIG_PATH, ENDPOINT_GROUPS_EXTENSION, properties),
         getPropertyOrReadConfig(SERDES, SERDES_CONFIG_PATH, SERDES_EXTENSION, properties));
  }

  private AletheiaConfig(final Properties properties,
                         final String endPointsConfig,
                         final String routingConfig,
                         final String endPointGroupsConfig,
                         final String serDeConfig) {

    this.properties = properties;

    Preconditions.checkNotNull(endPointsConfig, "endpoint config cannot be null");
    Preconditions.checkNotNull(routingConfig, "routing config cannot be null");
    Preconditions.checkNotNull(endPointGroupsConfig, "endpoint group config cannot be null");
    Preconditions.checkNotNull(serDeConfig, "serDe config cannot be null");

    if ("true".equalsIgnoreCase(properties.getProperty(VERIFY_ROUTING))) {
      verifyRoutingConfiguration(routingConfig, endPointGroupsConfig, endPointsConfig);
    }

    this.endPointsConfig = endPointsConfig;
    this.routingConfig = routingConfig;
    this.endPointGroupsConfig = endPointGroupsConfig;
    this.serDeConfig = serDeConfig;

    objectMapper = createObjectMapper();
  }

  private static void verifyRoutingConfiguration(final String routingConfig, final String endPointGroupsConfig, final String endPointsConfig) {
    try {
      final ObjectReader reader = simpleObjectMapper.reader();
      final JsonNode routingJson = reader.readTree(routingConfig);
      final JsonNode routingGroupsJson = reader.readTree(endPointGroupsConfig);
      final JsonNode endpointsJson = reader.readTree(endPointsConfig);

      Iterator<JsonNode> routingIterator = routingJson.elements();
      while (routingIterator.hasNext()) {
        final JsonNode routeJson = routingIterator.next();
        final JsonNode routeGroups = routeJson.get("routeGroups");
        if (!(routeGroups instanceof ArrayNode)) {
          throw new RuntimeException("RouteGroups " + routeGroups + " are not defined properly in routing configuration");
        }
        for (JsonNode group : routeGroups) {
          final String groupName = group.asText();
          if (groupName.contains("$")) {
            continue;
          }
          final JsonNode groupNameJson = routingGroupsJson.get(groupName);
          Preconditions.checkNotNull(groupNameJson, "Routing group " + groupName + " is not defined in endpoint groups configuration");
          if (!(groupNameJson instanceof ArrayNode)) {
            throw new RuntimeException("Routing group " + groupName + " is not defined properly in endpoint groups configuration");
          }
          for (JsonNode endpoint : groupNameJson) {
            final String endpointName = endpoint.get("endpoint").asText();
            if (!endpointName.contains("$")) {
              Preconditions.checkNotNull(endpointsJson.get(endpointName), "Endpoint " + endpointName + " is not defined properly in endpoints configuration");
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error checking routing configuration");
    }
  }

  private static String getPropertyOrReadConfig(final String property,
                                                final String configPathProperty,
                                                final String extensionProperty,
                                                final Properties properties) {
    if (!Strings.isNullOrEmpty(properties.getProperty(property))) {
      return properties.getProperty(property);
    }

    final Set<String> configPaths = new HashSet<>();
    configPaths.add(properties.getProperty(configPathProperty));

    final String multipleConfigurationPath = properties.getProperty(MULTIPLE_CONFIGURATIONS_PATH);
    final String extension = extensionProperty != null ? properties.getProperty(extensionProperty) : null;

    if (!Strings.isNullOrEmpty(multipleConfigurationPath) && !Strings.isNullOrEmpty(extension)) {
      try {
        final List<URL> resourceUrls = Collections.list(ClassLoader.getSystemResources(multipleConfigurationPath));
        for (URL url : resourceUrls) {
          final File folder = new File(url.getPath());
          final File[] files = folder.listFiles();
          if (files != null) {
            for (final File file : files) {
              if (file.isFile() && file.getName().endsWith(extension)) {
                configPaths.add(multipleConfigurationPath + "/" + file.getName());
              }
            }
          }
        }
      }
      catch (IOException e) {
        logger.error("Could not read resources", e);
      }
    }

    return readConfigFiles(configPaths);
  }

  Properties getProperties() {
    return properties;
  }

  private ObjectMapper createObjectMapper() {
    final ObjectMapper jsonMapper = new ObjectMapper();

//    jsonMapper.registerSubtypes(new NamedType(KafkaTopicEndPointTemplate.class,
//                                              KafkaTopicEndPointTemplate.TYPE));
//    jsonMapper.registerSubtypes(new NamedType(LogFileProductionEndPointTemplate.class,
//                                              LogFileProductionEndPointTemplate.TYPE));
//    jsonMapper.registerSubtypes(new NamedType(HiveTableEndPointTemplate.class,
//                                              HiveTableEndPointTemplate.TYPE));

    jsonMapper.registerSubtypes(new NamedType(InMemoryEndPointTemplate.class, InMemoryEndPointTemplate.TYPE));

    for (final Map.Entry<String, Class<? extends EndPointTemplate>> custom : customEndPointTemplates.entrySet()) {
      jsonMapper.registerSubtypes(new NamedType(custom.getValue(), custom.getKey()));
    }

    return jsonMapper;
  }


  private String resolve(final String configurationWithPlaceholders, final Properties placeholderValues)
          throws IOException {

    return new StrSubstitutor(
            new StrLookup<String>() {
              @Override
              public String lookup(final String key) {
                Preconditions.checkNotNull(placeholderValues.getProperty(key),
                                           "placeholder: \"${%s}\" was not assigned",
                                           key);
                return placeholderValues.getProperty(key);
              }
            },
            "${",
            "}",
            '#')
            .replace(configurationWithPlaceholders);
  }

  private static String readConfigFiles(final Set<String> configPaths) {
    if (configPaths == null || configPaths.isEmpty()) {
      throw new RuntimeException("No Aletheia config files to load");
    }

    final ObjectReader simpleObjectReader = simpleObjectMapper.reader();
    final JsonNode mergedConfig = simpleObjectReader.createObjectNode();

    for (final String resourcePath : configPaths) {
      if (!Strings.isNullOrEmpty(resourcePath)) {
        logger.info("Loading Aletheia config from " + resourcePath);
        try {
          final InputStream resourceAsStream = AletheiaConfig.class.getClassLoader().getResourceAsStream(resourcePath);
          final JsonNode json = simpleObjectReader.readTree(resourceAsStream);

          final Iterator<Map.Entry<String, JsonNode>> fieldsIterator = json.fields();
          while (fieldsIterator.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fieldsIterator.next();
            final String key = entry.getKey();
            if (mergedConfig.has(key)) {
              throw new RuntimeException("Element " + key + " in file " + resourcePath + " exists in previously loaded Aletheia configuration");
            }
            ((ObjectNode) mergedConfig).set(key, entry.getValue());
          }
        } catch (final IOException e) {
          logger.error("Failed to read config from " + resourcePath, e);
        }
      }
    }
    return mergedConfig.toString();
  }

  private <T> T readSingleConfigKey(final String config,
                                    final String key,
                                    final Properties placeholders,
                                    final TypeReference<T> typeReference) {
    return readSingleConfigKey(config, key, Functions.<JsonNode>identity(), placeholders, typeReference);
  }

  private <T> T readSingleConfigKey(final String config,
                                    final String key,
                                    final Function<JsonNode, JsonNode> transformation,
                                    final Properties placeholders,
                                    final TypeReference<T> typeReference) {
    try {
      final JsonNode configSectionNode = objectMapper.readTree(config).get(key);
      final String configSectionString = transformation.apply(configSectionNode).toString();
      final String resolvedConfigSectionString = resolve(configSectionString, placeholders);
      return objectMapper.readValue(resolvedConfigSectionString, typeReference);
    } catch (final Exception e) {
      throw new RuntimeException(String.format("error while reading configuration key: \"%s\" in config file: \"%s\"",
                                               key,
                                               config),
                                 e);
    }
  }

  public RoutingInfo getRouting(final String datumTypeId) {
    final ExtendedRoutingInfo extendedRoutingInfo = readSingleConfigKey(routingConfig,
                                                                        datumTypeId,
                                                                        properties,
                                                                        new TypeReference<ExtendedRoutingInfo>() {
                                                                        });
    final ImmutableList<List<Route>> expendedRouteGroupIds =
            FluentIterable.from(extendedRoutingInfo.getRouteGroupIds())
                          .transform(new Function<String, List<Route>>() {
                            @Override
                            public List<Route> apply(final String routeGroupId) {
                              return readSingleConfigKey(endPointGroupsConfig,
                                                         routeGroupId,
                                                         properties,
                                                         new TypeReference<List<Route>>() {
                                                         });
                            }
                          })
                          .toList();

    final ArrayList<Route> allRoutes =
            Lists.newArrayList(Iterables.concat(extendedRoutingInfo.getRoutes(),
                                                Iterables.concat(expendedRouteGroupIds)));

    return new RoutingInfo(allRoutes,
                           extendedRoutingInfo.getDatumKeySelectorClassName());
  }

  public <TDomainClass> DatumSerDe<TDomainClass> serDe(final String serDeId) {
    return readSingleConfigKey(serDeConfig,
                               serDeId,
                               properties,
                               new TypeReference<DatumSerDe<TDomainClass>>() {
                               });
  }

  public ProductionEndPoint getProductionEndPoint(final String endPointId) {
    final EndPointTemplate endPointTemplate =
            readSingleConfigKey(endPointsConfig,
                                endPointId,
                                new Function<JsonNode, JsonNode>() {
                                  @Override
                                  public JsonNode apply(final JsonNode jsonNode) {
                                    ((ObjectNode) jsonNode).remove(CONSUME_SECTION);
                                    return jsonNode;
                                  }
                                },
                                properties,
                                new TypeReference<EndPointTemplate>() {
                                });
    return endPointTemplate.getProductionEndPoint(endPointId);
  }

  public String getBreadcrumbEndPointId(final String datumTypeId) {
    return readSingleConfigKey(routingConfig,
                               datumTypeId,
                               new Function<JsonNode, JsonNode>() {
                                 @Override
                                 public JsonNode apply(final JsonNode jsonNode) {
                                   return jsonNode.get(BREADCRUMBS) != null ?
                                          jsonNode.get(BREADCRUMBS) :
                                          JsonNodeFactory.instance.textNode("");
                                 }
                               },
                               properties,
                               new TypeReference<String>() {
                               });
  }

  public ProductionEndPoint getBreadcrumbsProductionEndPoint(final String datumTypeId) {
    final String breadcrumbEndPointId = getBreadcrumbEndPointId(datumTypeId);
    return !Strings.isNullOrEmpty(breadcrumbEndPointId) ? getProductionEndPoint(breadcrumbEndPointId) : null;
  }

  private ConsumptionEndPoint getBreadcrumbsConsumptionEndPoint(final String datumTypeId) {
    final String breadcrumbEndPointId = getBreadcrumbEndPointId(datumTypeId);
    return !Strings.isNullOrEmpty(breadcrumbEndPointId) ? getConsumptionEndPoint(breadcrumbEndPointId) : null;
  }

  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointId) {
    final EndPointTemplate endPointTemplate =
            readSingleConfigKey(endPointsConfig,
                                endPointId,
                                new Function<JsonNode, JsonNode>() {
                                  @Override
                                  public JsonNode apply(final JsonNode jsonNode) {
                                    ((ObjectNode) jsonNode).remove(PRODUCE_SECTION);
                                    return jsonNode;
                                  }
                                },
                                properties,
                                new TypeReference<EndPointTemplate>() {
                                });
    return endPointTemplate.getConsumptionEndPoint(endPointId);
  }

  public DatumProducerConfig getDatumProducerConfig() {
    Preconditions.checkNotNull(properties.getProperty("aletheia.producer.source"),
                               "aletheia.producer.source must not be null");
    return new DatumProducerConfig(Integer.parseInt(properties.getProperty("aletheia.producer.incarnation", "1")),
                                   properties.getProperty("aletheia.producer.source"));
  }

  public DatumConsumerStreamConfig getDatumConsumerConfig() {
    Preconditions.checkNotNull(properties.getProperty("aletheia.consumer.source"),
                               "aletheia.consumer.source must not be null");
    return new DatumConsumerStreamConfig(Integer.parseInt(properties.getProperty("aletheia.consumer.incarnation", "1")),
                                         properties.getProperty("aletheia.consumer.source"));
  }

  public static void registerEndPointTemplate(final String type,
                                              final Class<? extends EndPointTemplate> endPointTemplateClass) {
    customEndPointTemplates.put(type, endPointTemplateClass);
  }

}

