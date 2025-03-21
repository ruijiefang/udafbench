//https://raw.githubusercontent.com/PAMunb/RVSec-replication-package/d0349919e991a3519cee8eacd290ca4ac31a29b5/ApacheCryptoAPIBench/apache_codes/spark/core/.evosuite/tmp_2022_04_21_21_19_14/tests/org/apache/spark/api/python/PythonFunction_ESTest_scaffolding.java
/**
 * Scaffolding file used to store all the setups needed to run 
 * tests automatically generated by EvoSuite
 * Thu Apr 21 21:49:30 GMT 2022
 */

package org.apache.spark.api.python;

import org.evosuite.runtime.annotation.EvoSuiteClassExclude;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.junit.AfterClass;
import org.evosuite.runtime.sandbox.Sandbox;
import org.evosuite.runtime.sandbox.Sandbox.SandboxMode;

import static org.evosuite.shaded.org.mockito.Mockito.*;
@EvoSuiteClassExclude
public class PythonFunction_ESTest_scaffolding {

  @org.junit.Rule
  public org.evosuite.runtime.vnet.NonFunctionalRequirementRule nfr = new org.evosuite.runtime.vnet.NonFunctionalRequirementRule();

  private static final java.util.Properties defaultProperties = (java.util.Properties) java.lang.System.getProperties().clone(); 

  private org.evosuite.runtime.thread.ThreadStopper threadStopper =  new org.evosuite.runtime.thread.ThreadStopper (org.evosuite.runtime.thread.KillSwitchHandler.getInstance(), 3000);


  @BeforeClass
  public static void initEvoSuiteFramework() { 
    org.evosuite.runtime.RuntimeSettings.className = "org.apache.spark.api.python.PythonFunction"; 
    org.evosuite.runtime.GuiSupport.initialize(); 
    org.evosuite.runtime.RuntimeSettings.maxNumberOfThreads = 100; 
    org.evosuite.runtime.RuntimeSettings.maxNumberOfIterationsPerLoop = 10000; 
    org.evosuite.runtime.RuntimeSettings.mockSystemIn = true; 
    org.evosuite.runtime.RuntimeSettings.sandboxMode = org.evosuite.runtime.sandbox.Sandbox.SandboxMode.RECOMMENDED; 
    org.evosuite.runtime.sandbox.Sandbox.initializeSecurityManagerForSUT(); 
    org.evosuite.runtime.classhandling.JDKClassResetter.init();
    setSystemProperties();
    initializeClasses();
    org.evosuite.runtime.Runtime.getInstance().resetRuntime(); 
    try { initMocksToAvoidTimeoutsInTheTests(); } catch(ClassNotFoundException e) {} 
  } 

  @AfterClass
  public static void clearEvoSuiteFramework(){ 
    Sandbox.resetDefaultSecurityManager(); 
    java.lang.System.setProperties((java.util.Properties) defaultProperties.clone()); 
  } 

  @Before
  public void initTestCase(){ 
    threadStopper.storeCurrentThreads();
    threadStopper.startRecordingTime();
    org.evosuite.runtime.jvm.ShutdownHookHandler.getInstance().initHandler(); 
    org.evosuite.runtime.sandbox.Sandbox.goingToExecuteSUTCode(); 
    setSystemProperties(); 
    org.evosuite.runtime.GuiSupport.setHeadless(); 
    org.evosuite.runtime.Runtime.getInstance().resetRuntime(); 
    org.evosuite.runtime.agent.InstrumentingAgent.activate(); 
  } 

  @After
  public void doneWithTestCase(){ 
    threadStopper.killAndJoinClientThreads();
    org.evosuite.runtime.jvm.ShutdownHookHandler.getInstance().safeExecuteAddedHooks(); 
    org.evosuite.runtime.classhandling.JDKClassResetter.reset(); 
    resetClasses(); 
    org.evosuite.runtime.sandbox.Sandbox.doneWithExecutingSUTCode(); 
    org.evosuite.runtime.agent.InstrumentingAgent.deactivate(); 
    org.evosuite.runtime.GuiSupport.restoreHeadlessMode(); 
  } 

  public static void setSystemProperties() {
 
    java.lang.System.setProperties((java.util.Properties) defaultProperties.clone()); 
    java.lang.System.setProperty("file.encoding", "UTF-8"); 
    java.lang.System.setProperty("java.awt.headless", "true"); 
    java.lang.System.setProperty("java.io.tmpdir", "/tmp"); 
    java.lang.System.setProperty("user.dir", "/home/pedro/projects/RVSec-replication-package/ApacheCryptoAPIBench/apache_codes/spark/core"); 
    java.lang.System.setProperty("user.home", "/home/pedro"); 
    java.lang.System.setProperty("user.language", "en"); 
    java.lang.System.setProperty("user.name", "pedro"); 
    java.lang.System.setProperty("user.timezone", "Etc/UTC"); 
  }

  private static void initializeClasses() {
    org.evosuite.runtime.classhandling.ClassStateSupport.initializeClasses(PythonFunction_ESTest_scaffolding.class.getClassLoader() ,
      "scala.collection.Seq$",
      "scala.runtime.BooleanRef",
      "scala.math.Equiv",
      "scala.collection.mutable.Iterable$class",
      "org.apache.spark.scheduler.AccumulableInfo",
      "scala.Function1$class",
      "scala.collection.generic.Shrinkable",
      "scala.collection.mutable.Seq$class",
      "scala.collection.LinearSeqOptimized",
      "scala.collection.Iterator$class",
      "scala.collection.generic.IndexedSeqFactory",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcI$sp",
      "scala.collection.AbstractMap",
      "scala.Product7$class",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcC$sp",
      "scala.collection.immutable.IndexedSeq",
      "scala.collection.immutable.StringLike",
      "scala.collection.Map",
      "scala.Function7$class",
      "org.apache.spark.SparkContext",
      "scala.collection.mutable.HashTable",
      "scala.collection.mutable.IndexedSeqLike",
      "scala.Option$WithFilter",
      "scala.collection.mutable.Buffer",
      "scala.Product$class",
      "scala.collection.generic.FilterMonadic",
      "scala.runtime.ScalaRunTime$",
      "scala.runtime.NonLocalReturnControl",
      "scala.collection.mutable.Cloneable",
      "scala.collection.generic.GenTraversableFactory",
      "scala.collection.SeqViewLike",
      "scala.util.hashing.MurmurHash3$ArrayHashing",
      "scala.collection.GenIterableLike",
      "scala.Equals",
      "scala.collection.GenIterable$class",
      "scala.collection.generic.GenericSetTemplate",
      "scala.collection.mutable.StringBuilder",
      "scala.collection.generic.GenericTraversableTemplate",
      "scala.Mutable",
      "scala.math.Ordering",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcS$sp",
      "scala.collection.GenSet",
      "org.apache.spark.api.python.PythonFunction",
      "scala.collection.GenTraversable",
      "scala.collection.GenSeq",
      "org.apache.spark.util.AccumulatorMetadata",
      "scala.collection.GenSeqLike",
      "org.apache.spark.internal.Logging",
      "scala.collection.immutable.Vector",
      "scala.collection.mutable.Seq",
      "scala.collection.SeqLike",
      "scala.collection.LinearSeq",
      "scala.collection.TraversableView",
      "scala.collection.GenSeq$",
      "scala.collection.LinearSeqLike",
      "scala.collection.mutable.SeqLike",
      "scala.util.Left",
      "scala.collection.immutable.Nil$",
      "org.apache.spark.api.python.PythonBroadcast",
      "scala.Function7$$anonfun$curried$1",
      "scala.Predef$$less$colon$less",
      "scala.PartialFunction",
      "scala.collection.generic.GenericTraversableTemplate$class",
      "scala.Tuple7",
      "scala.collection.Seq$class",
      "scala.collection.IndexedSeqLike$class",
      "scala.collection.TraversableOnce",
      "scala.Tuple2",
      "scala.collection.mutable.IndexedSeqLike$class",
      "scala.Function7$$anonfun$tupled$1",
      "scala.collection.IterableView",
      "scala.collection.BufferedIterator",
      "scala.runtime.AbstractFunction1",
      "scala.collection.immutable.Seq",
      "scala.collection.mutable.Iterable",
      "scala.collection.immutable.Set",
      "scala.runtime.AbstractFunction7",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcB$sp",
      "scala.collection.mutable.IndexedSeq$class",
      "scala.collection.mutable.IndexedSeqView",
      "scala.collection.IndexedSeq$",
      "scala.collection.IndexedSeqLike",
      "scala.collection.mutable.IndexedSeqOptimized",
      "scala.collection.mutable.MapLike",
      "scala.Product7",
      "scala.collection.GenTraversableLike",
      "scala.Product2",
      "scala.math.Numeric",
      "scala.collection.GenIterable$",
      "scala.collection.mutable.HashMap",
      "scala.util.control.NoStackTrace",
      "scala.util.Right",
      "scala.collection.generic.TraversableFactory",
      "scala.collection.mutable.IndexedSeq",
      "scala.PartialFunction$class",
      "scala.util.hashing.MurmurHash3",
      "scala.runtime.BoxedUnit",
      "org.apache.spark.util.CollectionAccumulator",
      "scala.Option",
      "scala.collection.SeqLike$$anon$1",
      "scala.collection.generic.Subtractable",
      "scala.collection.IndexedSeq$class",
      "scala.collection.mutable.Traversable",
      "scala.collection.mutable.AbstractMap",
      "scala.collection.Parallelizable",
      "scala.Serializable",
      "scala.reflect.ScalaSignature",
      "scala.collection.immutable.Stream$Cons",
      "org.apache.spark.api.python.PythonFunction$",
      "scala.collection.mutable.BufferLike",
      "scala.collection.AbstractSeq",
      "scala.runtime.BoxesRunTime",
      "scala.collection.Iterable$",
      "org.apache.spark.broadcast.Broadcast",
      "scala.collection.immutable.List",
      "scala.collection.TraversableLike$class",
      "scala.collection.Seq",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcV$sp",
      "scala.collection.Set",
      "scala.collection.generic.GenericCompanion",
      "scala.collection.mutable.SeqLike$class",
      "scala.collection.immutable.VectorPointer",
      "scala.collection.immutable.LinearSeq",
      "scala.util.control.ControlThrowable",
      "scala.math.PartialOrdering",
      "scala.collection.GenTraversable$",
      "scala.collection.MapLike",
      "scala.collection.IndexedSeqOptimized$class",
      "scala.collection.mutable.Builder",
      "scala.collection.immutable.Stream$Empty$",
      "scala.collection.Traversable$class",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcF$sp",
      "scala.collection.mutable.Iterable$",
      "scala.reflect.ClassTag",
      "scala.Function0",
      "scala.Function1",
      "scala.Function2",
      "scala.Function6",
      "scala.Function7",
      "scala.collection.GenMapLike",
      "scala.collection.ViewMkString",
      "scala.collection.GenMap",
      "scala.util.Either",
      "scala.collection.Parallelizable$class",
      "scala.collection.generic.GenericSeqCompanion",
      "scala.math.Ordered$class",
      "scala.collection.SeqView",
      "scala.collection.SeqLike$class",
      "scala.collection.mutable.Builder$class",
      "scala.None$",
      "scala.collection.TraversableViewLike",
      "scala.collection.mutable.Map",
      "scala.collection.immutable.Stream",
      "scala.Product",
      "scala.collection.GenTraversable$class",
      "scala.collection.Iterator$GroupedIterator",
      "scala.collection.immutable.Iterable",
      "org.apache.spark.api.python.PythonAccumulatorV2",
      "scala.collection.mutable.HashTable$HashUtils",
      "scala.reflect.ClassManifestDeprecatedApis",
      "scala.collection.GenIterable",
      "scala.collection.script.Scriptable",
      "scala.collection.immutable.StringLike$class",
      "scala.collection.GenSeqLike$class",
      "scala.collection.generic.SeqFactory",
      "scala.Cloneable",
      "scala.collection.mutable.Cloneable$class",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcZ$sp",
      "scala.Some",
      "scala.MatchError",
      "scala.collection.generic.GenSeqFactory",
      "scala.reflect.OptManifest",
      "scala.collection.TraversableOnce$class",
      "scala.collection.GenSeq$class",
      "scala.math.Ordered",
      "scala.UninitializedError",
      "scala.collection.TraversableLike",
      "scala.collection.SetLike",
      "scala.collection.IterableLike",
      "scala.collection.immutable.Map",
      "org.apache.spark.util.AccumulatorV2",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcJ$sp",
      "scala.collection.generic.CanBuildFrom",
      "scala.util.hashing.MurmurHash3$ArrayHashing$mcD$sp",
      "scala.collection.GenSetLike",
      "scala.collection.IndexedSeq",
      "scala.collection.IterableViewLike",
      "scala.util.hashing.MurmurHash3$",
      "scala.collection.mutable.Traversable$",
      "scala.collection.generic.HasNewBuilder",
      "scala.collection.GenTraversableOnce",
      "scala.collection.AbstractTraversable",
      "scala.collection.immutable.MapLike",
      "scala.collection.AbstractIterator",
      "scala.Immutable",
      "scala.collection.AbstractIterable",
      "scala.runtime.ScalaRunTime$$anon$1",
      "org.apache.spark.SparkException",
      "scala.collection.Traversable",
      "scala.collection.immutable.$colon$colon",
      "scala.collection.immutable.Traversable",
      "scala.util.hashing.Hashing",
      "scala.collection.generic.Growable$class",
      "scala.runtime.Nothing$",
      "scala.collection.Iterator",
      "scala.collection.TraversableOnce$$anonfun$addString$1",
      "scala.collection.IndexedSeqOptimized",
      "scala.collection.mutable.Seq$",
      "scala.collection.Traversable$",
      "scala.collection.Iterable",
      "scala.collection.mutable.AbstractSeq",
      "scala.collection.mutable.IndexedSeq$",
      "scala.collection.generic.Clearable",
      "scala.collection.mutable.Traversable$class",
      "scala.collection.generic.Growable",
      "scala.collection.IterableLike$class",
      "scala.collection.Iterable$class",
      "scala.collection.CustomParallelizable"
    );
  } 
  private static void initMocksToAvoidTimeoutsInTheTests() throws ClassNotFoundException { 
    mock(Class.forName("org.apache.spark.broadcast.Broadcast", false, PythonFunction_ESTest_scaffolding.class.getClassLoader()));
  }

  private static void resetClasses() {
    org.evosuite.runtime.classhandling.ClassResetter.getInstance().setClassLoader(PythonFunction_ESTest_scaffolding.class.getClassLoader()); 

    org.evosuite.runtime.classhandling.ClassStateSupport.resetClasses(
      "org.apache.spark.api.python.PythonFunction",
      "scala.runtime.AbstractFunction7",
      "scala.Function7$class",
      "org.apache.spark.api.python.PythonFunction$",
      "scala.runtime.ScalaRunTime$",
      "scala.Product$class",
      "scala.collection.AbstractIterator",
      "scala.runtime.ScalaRunTime$$anon$1",
      "scala.collection.TraversableOnce$class",
      "scala.collection.Iterator$class",
      "scala.collection.AbstractTraversable",
      "scala.collection.AbstractIterable",
      "scala.collection.AbstractSeq",
      "scala.collection.mutable.AbstractSeq",
      "scala.collection.mutable.StringBuilder",
      "scala.collection.Parallelizable$class",
      "scala.collection.TraversableLike$class",
      "scala.collection.generic.GenericTraversableTemplate$class",
      "scala.collection.GenTraversable$class",
      "scala.collection.Traversable$class",
      "scala.collection.GenIterable$class",
      "scala.collection.IterableLike$class",
      "scala.collection.Iterable$class",
      "scala.Function1$class",
      "scala.PartialFunction$class",
      "scala.collection.GenSeqLike$class",
      "scala.collection.GenSeq$class",
      "scala.collection.SeqLike$class",
      "scala.collection.Seq$class",
      "scala.collection.mutable.Traversable$class",
      "scala.collection.mutable.Iterable$class",
      "scala.collection.mutable.Cloneable$class",
      "scala.collection.mutable.SeqLike$class",
      "scala.collection.mutable.Seq$class",
      "scala.collection.IndexedSeqLike$class",
      "scala.collection.IndexedSeq$class",
      "scala.collection.mutable.IndexedSeqLike$class",
      "scala.collection.mutable.IndexedSeq$class",
      "scala.collection.IndexedSeqOptimized$class",
      "scala.math.Ordered$class",
      "scala.collection.immutable.StringLike$class",
      "scala.collection.generic.Growable$class",
      "scala.collection.mutable.Builder$class",
      "scala.runtime.BooleanRef",
      "scala.runtime.AbstractFunction1",
      "scala.collection.TraversableOnce$$anonfun$addString$1",
      "scala.runtime.BoxedUnit",
      "org.apache.spark.util.AccumulatorV2",
      "org.apache.spark.util.CollectionAccumulator",
      "org.apache.spark.api.python.PythonAccumulatorV2",
      "org.apache.spark.internal.Logging$class",
      "org.apache.spark.util.SparkUncaughtExceptionHandler",
      "org.apache.spark.util.SparkUncaughtExceptionHandler$",
      "scala.sys.package$",
      "scala.collection.generic.GenMapFactory",
      "scala.collection.generic.MapFactory",
      "scala.collection.generic.ImmutableMapFactory",
      "scala.collection.immutable.Map$",
      "scala.collection.convert.DecorateAsJava$class",
      "scala.collection.convert.DecorateAsScala$class",
      "scala.collection.JavaConverters$",
      "scala.collection.convert.Decorators$AsScala",
      "scala.collection.convert.Decorators$class",
      "scala.collection.convert.Decorators$",
      "scala.runtime.AbstractFunction0",
      "scala.collection.convert.DecorateAsScala$$anonfun$mapAsScalaMapConverter$1",
      "scala.Function0$class",
      "scala.collection.convert.WrapAsScala$class",
      "scala.collection.convert.WrapAsScala$",
      "scala.collection.AbstractMap",
      "scala.collection.mutable.AbstractMap",
      "scala.collection.convert.Wrappers$JMapWrapper",
      "scala.collection.convert.Wrappers$class",
      "scala.collection.convert.Wrappers$",
      "scala.collection.GenMapLike$class",
      "scala.collection.generic.Subtractable$class",
      "scala.collection.MapLike$class",
      "scala.collection.Map$class",
      "scala.collection.generic.Shrinkable$class",
      "scala.collection.mutable.MapLike$class",
      "scala.collection.mutable.Map$class",
      "scala.collection.convert.Wrappers$JMapWrapperLike$class",
      "scala.collection.mutable.AbstractBuffer",
      "scala.collection.mutable.ArrayBuffer",
      "scala.collection.mutable.BufferLike$class",
      "scala.collection.mutable.Buffer$class",
      "scala.collection.mutable.ResizableArray$class",
      "scala.math.package$",
      "scala.collection.CustomParallelizable$class",
      "scala.collection.generic.Growable$$anonfun$$plus$plus$eq$1",
      "scala.collection.convert.Wrappers$JMapWrapperLike$$anon$2",
      "scala.Tuple2",
      "scala.Product2$class",
      "scala.collection.mutable.MapBuilder",
      "scala.collection.immutable.AbstractMap",
      "scala.collection.immutable.Traversable$class",
      "scala.collection.immutable.Iterable$class",
      "scala.collection.immutable.MapLike$class",
      "scala.collection.immutable.Map$class",
      "scala.collection.immutable.Map$EmptyMap$",
      "scala.collection.immutable.Map$Map1",
      "scala.collection.immutable.Map$Map2",
      "scala.collection.immutable.Map$Map3",
      "scala.collection.immutable.Map$Map4",
      "scala.collection.immutable.HashMap",
      "scala.LowPriorityImplicits",
      "scala.DeprecatedPredef$class",
      "scala.package$$anon$1",
      "scala.collection.generic.GenericCompanion",
      "scala.collection.generic.GenTraversableFactory",
      "scala.collection.generic.GenTraversableFactory$GenericCanBuildFrom",
      "scala.collection.generic.GenTraversableFactory$$anon$1",
      "scala.util.control.Breaks",
      "scala.util.control.BreakControl",
      "scala.util.control.NoStackTrace$class",
      "scala.sys.SystemProperties$",
      "scala.sys.BooleanProp$",
      "scala.sys.PropImpl",
      "scala.sys.BooleanProp$BooleanPropImpl",
      "scala.sys.BooleanProp$$anonfun$keyExists$1",
      "scala.collection.generic.MutableMapFactory",
      "scala.collection.mutable.Map$",
      "scala.collection.immutable.List",
      "scala.collection.immutable.Seq$class",
      "scala.collection.LinearSeqLike$class",
      "scala.collection.LinearSeq$class",
      "scala.collection.immutable.LinearSeq$class",
      "scala.collection.LinearSeqOptimized$class",
      "scala.collection.immutable.Nil$",
      "scala.collection.mutable.HashMap",
      "scala.collection.mutable.HashTable$HashUtils$class",
      "scala.collection.mutable.HashTable$class",
      "scala.collection.mutable.HashTable$",
      "scala.util.hashing.package$",
      "scala.collection.mutable.DefaultEntry",
      "scala.collection.mutable.HashEntry$class",
      "scala.Option",
      "scala.None$",
      "scala.sys.SystemProperties",
      "scala.runtime.AbstractFunction0$mcZ$sp",
      "scala.sys.SystemProperties$$anonfun$contains$1",
      "scala.Function0$mcZ$sp$class",
      "scala.Some",
      "scala.sys.SystemProperties$$anonfun$get$1",
      "scala.Option$",
      "scala.runtime.BoxesRunTime",
      "scala.util.control.NoStackTrace$",
      "scala.collection.Traversable$",
      "scala.collection.Iterable$",
      "scala.collection.generic.GenSeqFactory",
      "scala.collection.generic.SeqFactory",
      "scala.collection.Seq$",
      "scala.collection.generic.IndexedSeqFactory",
      "scala.collection.IndexedSeq$$anon$1",
      "scala.collection.IndexedSeq$",
      "scala.collection.Iterator$$anon$2",
      "scala.collection.Iterator$",
      "scala.collection.immutable.List$$anon$1",
      "scala.collection.immutable.List$",
      "scala.collection.immutable.$colon$colon$",
      "scala.collection.$plus$colon$",
      "scala.collection.$colon$plus$",
      "scala.collection.immutable.Stream$",
      "scala.collection.immutable.Stream$$hash$colon$colon$",
      "scala.collection.immutable.Vector",
      "scala.collection.immutable.IndexedSeq$class",
      "scala.collection.immutable.VectorPointer$class",
      "scala.collection.immutable.Vector$",
      "scala.collection.mutable.StringBuilder$",
      "scala.collection.immutable.Range$",
      "scala.math.BigDecimal$",
      "scala.math.BigInt$",
      "scala.math.LowPriorityEquiv$class",
      "scala.math.Equiv$",
      "scala.math.Fractional$",
      "scala.math.Integral$",
      "scala.math.Numeric$",
      "scala.math.Ordered$",
      "scala.math.LowPriorityOrderingImplicits$class",
      "scala.math.Ordering$",
      "scala.util.Either$",
      "scala.util.Left$",
      "scala.util.Right$",
      "scala.package$",
      "scala.collection.generic.GenSetFactory",
      "scala.collection.generic.SetFactory",
      "scala.collection.generic.ImmutableSetFactory",
      "scala.collection.immutable.Set$",
      "scala.reflect.AnyValManifest",
      "scala.reflect.ManifestFactory$$anon$6",
      "scala.reflect.ClassManifestDeprecatedApis$class",
      "scala.reflect.ClassTag$class",
      "scala.reflect.Manifest$class",
      "scala.reflect.ManifestFactory$$anon$7",
      "scala.reflect.ManifestFactory$$anon$8",
      "scala.reflect.ManifestFactory$$anon$9",
      "scala.reflect.ManifestFactory$$anon$10",
      "scala.reflect.ManifestFactory$$anon$11",
      "scala.reflect.ManifestFactory$$anon$12",
      "scala.reflect.ManifestFactory$$anon$13",
      "scala.reflect.ManifestFactory$$anon$14",
      "scala.reflect.ManifestFactory$ClassTypeManifest",
      "scala.reflect.ManifestFactory$PhantomManifest",
      "scala.reflect.ManifestFactory$$anon$1",
      "scala.reflect.ManifestFactory$$anon$2",
      "scala.reflect.ManifestFactory$$anon$3",
      "scala.reflect.ManifestFactory$$anon$4",
      "scala.reflect.ManifestFactory$$anon$5",
      "scala.reflect.ManifestFactory$",
      "scala.reflect.ClassManifestFactory$",
      "scala.reflect.package$",
      "scala.reflect.NoManifest$",
      "scala.Predef$$anon$3",
      "scala.Predef$$less$colon$less",
      "scala.Predef$$anon$1",
      "scala.Predef$$eq$colon$eq",
      "scala.Predef$$anon$2",
      "scala.Predef$",
      "scala.collection.mutable.WrappedArray",
      "scala.collection.mutable.WrappedArray$ofRef",
      "scala.collection.mutable.ArrayLike$class",
      "scala.collection.immutable.HashMap$HashMap1",
      "scala.collection.generic.BitOperations$Int$class",
      "scala.runtime.AbstractFunction2",
      "scala.collection.immutable.HashMap$$anonfun$1",
      "scala.Function2$class",
      "scala.collection.immutable.HashMap$Merger",
      "scala.collection.immutable.HashMap$$anon$2",
      "scala.collection.immutable.HashMap$$anon$2$$anon$3",
      "scala.collection.immutable.HashMap$",
      "scala.collection.immutable.HashMap$HashTrieMap",
      "scala.collection.generic.GenMapFactory$MapCanBuildFrom",
      "scala.collection.immutable.HashMap$EmptyHashMap$",
      "scala.FallbackArrayBuilding",
      "scala.Array$",
      "scala.util.PropertiesTrait$class",
      "scala.runtime.AbstractFunction0$mcV$sp",
      "scala.util.PropertiesTrait$$anonfun$scalaProps$1",
      "scala.Function0$mcV$sp$class",
      "scala.util.PropertiesTrait$$anonfun$scalaProps$2",
      "scala.util.PropertiesTrait$$anonfun$1",
      "scala.Option$WithFilter",
      "scala.util.PropertiesTrait$$anonfun$2",
      "scala.util.PropertiesTrait$$anonfun$3",
      "scala.util.PropertiesTrait$$anonfun$4",
      "scala.util.PropertiesTrait$$anonfun$scalaPropOrElse$1",
      "scala.util.Properties$",
      "scala.compat.Platform$",
      "org.apache.commons.lang3.math.NumberUtils",
      "org.apache.commons.lang3.JavaVersion",
      "org.apache.commons.lang3.SystemUtils",
      "scala.collection.immutable.StringOps",
      "scala.util.matching.Regex",
      "scala.collection.immutable.StringOps$",
      "scala.collection.AbstractSet",
      "scala.collection.GenSetLike$class",
      "scala.collection.generic.GenericSetTemplate$class",
      "scala.collection.GenSet$class",
      "scala.collection.SetLike$class",
      "scala.collection.Set$class",
      "scala.collection.immutable.Set$class",
      "scala.collection.immutable.Set$EmptySet$",
      "org.apache.spark.util.Utils$",
      "org.apache.spark.util.Utils$$anonfun$checkHost$1",
      "org.apache.spark.SparkEnv$",
      "scala.Function7$$anonfun$curried$1",
      "scala.Function7$$anonfun$tupled$1",
      "scala.StringContext",
      "scala.collection.mutable.WrappedArray$",
      "scala.StringContext$$anonfun$s$1",
      "scala.collection.IndexedSeqLike$Elements",
      "scala.collection.BufferedIterator$class",
      "scala.StringContext$",
      "org.apache.spark.broadcast.Broadcast",
      "scala.util.hashing.MurmurHash3",
      "scala.util.hashing.MurmurHash3$",
      "scala.Tuple7",
      "scala.Product7$class",
      "scala.Enumeration",
      "scala.collection.immutable.BitSet",
      "scala.Enumeration$ValueSet",
      "scala.collection.generic.Sorted$class",
      "scala.collection.SortedSetLike$class",
      "scala.collection.SortedSet$class",
      "scala.collection.immutable.SortedSet$class",
      "scala.runtime.IntRef",
      "scala.collection.TraversableOnce$$anonfun$size$1",
      "scala.Enumeration$ValueSet$$anonfun$iterator$1",
      "scala.PartialFunction$AndThen",
      "org.apache.spark.broadcast.TorrentBroadcast",
      "org.apache.spark.api.python.PythonBroadcast"
    );
  }
}
