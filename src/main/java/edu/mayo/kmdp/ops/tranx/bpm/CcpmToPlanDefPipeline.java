/**
 * Copyright © 2018 Mayo Clinic (RSTKNOWLEDGEMGMT@mayo.edu)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.mayo.kmdp.ops.tranx.bpm;

import static java.util.Arrays.asList;
import static org.omg.spec.api4kp._20200801.AbstractCarrier.rep;
import static org.omg.spec.api4kp._20200801.services.transrepresentation.ModelMIMECoder.encode;
import static org.omg.spec.api4kp._20200801.taxonomy.krlanguage.KnowledgeRepresentationLanguageSeries.FHIR_STU3;
import static org.omg.spec.api4kp._20200801.taxonomy.lexicon.LexiconSeries.PCV;
import static org.omg.spec.api4kp._20200801.taxonomy.lexicon.LexiconSeries.SNOMED_CT;
import static org.omg.spec.api4kp._20200801.taxonomy.parsinglevel.ParsingLevelSeries.Abstract_Knowledge_Expression;

import edu.mayo.kmdp.knowledgebase.KnowledgeBaseProvider;
import edu.mayo.kmdp.knowledgebase.assemblers.rdf.GraphBasedAssembler;
import edu.mayo.kmdp.knowledgebase.constructors.DependencyBasedConstructor;
import edu.mayo.kmdp.knowledgebase.flatteners.fhir.stu3.PlanDefinitionFlattener;
import edu.mayo.kmdp.knowledgebase.weavers.fhir.stu3.DMNDefToPlanDefWeaver;
import edu.mayo.kmdp.language.LanguageDeSerializer;
import edu.mayo.kmdp.language.TransrepresentationExecutor;
import edu.mayo.kmdp.language.parsers.cmmn.v1_1.CMMN11Parser;
import edu.mayo.kmdp.language.parsers.dmn.v1_2.DMN12Parser;
import edu.mayo.kmdp.language.translators.cmmn.v1_1.CmmnToPlanDefTranslator;
import edu.mayo.kmdp.language.translators.dmn.v1_2.DmnToPlanDefTranslator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.PostConstruct;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.CompositionalApiInternal;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.TranscreateApiInternal._applyNamedTransform;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetCatalogApi;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetRepositoryApi;
import org.omg.spec.api4kp._20200801.api.terminology.v4.TermsApi;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.DeserializeApiInternal;
import org.omg.spec.api4kp._20200801.api.transrepresentation.v4.server.TransxionApiInternal;
import org.omg.spec.api4kp._20200801.id.ResourceIdentifier;
import org.omg.spec.api4kp._20200801.services.CompositeKnowledgeCarrier;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;
import org.springframework.beans.factory.annotation.Autowired;

public class CcpmToPlanDefPipeline implements _applyNamedTransform {

  public static final UUID id = UUID.fromString("77234f8c-718b-4429-b800-8b17369bc215");

  KnowledgeAssetCatalogApi cat;
  KnowledgeAssetRepositoryApi repo;
  TermsApi terms;

  ResourceIdentifier dictionaryAssetId;
  ResourceIdentifier dictionaryArtifactId;

  DeserializeApiInternal parser;

  TransxionApiInternal translator;

  KnowledgeBaseApiInternal._getKnowledgeBaseStructure constructor;

  CompositionalApiInternal._flattenArtifact flattener;

  CompositionalApiInternal._assembleCompositeArtifact assembler;

  KnowledgeBaseProvider kbManager;


  protected Map<Integer, Consumer<Answer<KnowledgeCarrier>>> injectors = new HashMap<>();

  public CcpmToPlanDefPipeline(
      ResourceIdentifier dictionaryAssetId,
      ResourceIdentifier dictionaryArtifactId,
      @Autowired KnowledgeAssetCatalogApi cat,
      @Autowired KnowledgeAssetRepositoryApi repo,
      @Autowired TermsApi terms
  ) {
    this.dictionaryAssetId = dictionaryAssetId;
    this.dictionaryArtifactId = dictionaryArtifactId;
    this.cat = cat;
    this.repo = repo;
    this.terms = terms;
    init();
  }


  @PostConstruct
  protected void init() {
    parser =
        new LanguageDeSerializer(asList(new DMN12Parser(), new CMMN11Parser()));

    translator = new TransrepresentationExecutor(
        asList(new CmmnToPlanDefTranslator(), new DmnToPlanDefTranslator())
    );

    constructor
        = DependencyBasedConstructor.newInstance(cat);

    flattener
        = new PlanDefinitionFlattener();

    assembler
        = GraphBasedAssembler.newInstance(repo);

    kbManager
        = new KnowledgeBaseProvider(repo)
        .withNamedWeaver(kbp -> new DMNDefToPlanDefWeaver(kbp, terms, dictionaryArtifactId.getResourceId()));
  }

  public CcpmToPlanDefPipeline addInjector(int index, Consumer<Answer<KnowledgeCarrier>> consumer) {
    injectors.put(index,consumer);
    return this;
  }

  public Consumer<Answer<KnowledgeCarrier>> injector(int j) {
    return injectors.getOrDefault(j, kc -> {});
  }


  @Override
  public Answer<KnowledgeCarrier> applyNamedTransform(UUID operatorId, UUID kbaseId,
      String versionTag, String xParams) {
    UUID rootAssetId = kbaseId;
    String rootAssetVersionTag = versionTag;

    // The dependency-based constructor considers the given asset as the root of a tree-based knowledge base,
    // implicitly defined by the query { caseModel dependsOn* decisionModel }
    // In particular, in this case we have
    //    { caseModel dependsOn* decisionModel }
    // The operation then returns a 'structure', which is effectively an 'intensional' manifestation of a new, composite Asset
    Answer<KnowledgeCarrier> struct =
        constructor.getKnowledgeBaseStructure(rootAssetId, rootAssetVersionTag);
    injector(0).accept(struct);

    // Now fetch the components to create a Composite Artifact
    Answer<KnowledgeCarrier> composite =
        struct.flatMap(kc -> assembler.assembleCompositeArtifact(kc));

    // Parse
    Answer<KnowledgeCarrier> parsedComposite =
        composite.flatMap(kc -> parser.applyLift(kc, Abstract_Knowledge_Expression));
    injector(1).accept(parsedComposite);

    // Weave the dictionary in
    // TODO Definitely need to smoothen out the APIs
    KnowledgeCarrier dictionary
        = repo.getCanonicalKnowledgeAssetCarrier(dictionaryAssetId.getUuid(),dictionaryAssetId.getVersionTag())
        .flatMap(kc -> parser.applyLift(kc,Abstract_Knowledge_Expression))
        .orElseThrow(IllegalStateException::new);

    Answer<KnowledgeCarrier> wovenComposite =
        parsedComposite.flatMap(kc ->
            // TODO: 03/12/2020 where we want to get to with id updates
            kbManager.initKnowledgeBase(kc)
                .flatMap(pid ->
                    kbManager.weave(pid.getUuid(), pid.getVersionTag(), dictionary))
                .flatMap(dId ->
                    kbManager.getKnowledgeBaseManifestation(dId.getUuid(), dId.getVersionTag())));

    injector(2).accept(wovenComposite);

    // Translate into PlanDefinition
    Answer<KnowledgeCarrier> planDefinitions =
        wovenComposite.flatMap(kc ->
            translator.applyTransrepresent(kc, encode(rep(FHIR_STU3, SNOMED_CT, PCV)), null))
            // TODO The translator should maybe preserve the AssetID ?
            .map(kc -> kc.withAssetId(struct.get().getAssetId()));
    injector(3).accept(planDefinitions);


    // Flatten the composite, which at this point is homogeneous FHIR PlanDef
    Answer<KnowledgeCarrier> planDefinition = planDefinitions
        .reduce(kc -> flattener.flattenArtifact((CompositeKnowledgeCarrier) kc, rootAssetId));
    injector(4).accept(planDefinition);

    // And finally unwrap...
    return planDefinition;
  }


}