{-# LANGUAGE TemplateHaskell, QuasiQuotes, OverloadedStrings #-}
{-# LANGUAGE TypeFamilies, EmptyDataDecls, GADTs #-}
module Main where

import Database.Persist
import Database.Persist.Zookeeper
import Database.Persist.TH
import Language.Haskell.TH.Syntax
import Data.Maybe
import Test.Hspec

let zookeeperSettings = mkPersistSettings (ConT ''ZookeeperBackend)
 in share [mkPersist zookeeperSettings] [persistLowerCase|
Person
    name String
    age Int
    PersonU name
    deriving Show
    deriving Eq
|]

zookeeperConf :: ZookeeperConf
zookeeperConf = ZookeeperConf "localhost:2181" 10000 1 50 30

main :: IO ()
main = 
  withZookeeperConn zookeeperConf $ \conn -> do
    hspec $ do
      let key = (Key (PersistText "/person/WyJzVGVzdC9ob2dlIl0="))
      let val = Person "Test/hoge" 12
      describe "PersistUnique test" $ do
        it "insertUnique" $ do
          print $ show $ filterClause val [PersonName ==. ""]
          print $ show $ filterClause val [PersonAge <=. 100]
          v <- flip runZookeeperPool conn $ do
            deleteBy $ PersonU "Test/hoge"
            insertUnique val
          v `shouldBe` (Just key)
        it "getBy" $ do
          v <- flip runZookeeperPool conn $ do
            getBy $ PersonU "Test/hoge" 
          (entityKey (fromJust v)) `shouldBe` key
          (entityVal (fromJust v)) `shouldBe` val
          v `shouldBe` (Just (Entity key val))
        it "deleteBy" $ do
          v <- flip runZookeeperPool conn $ do
            deleteBy $ PersonU "Test/hoge"
            getBy $ PersonU "Test/hoge"
          v `shouldBe` Nothing
      describe "PersistStore test" $ do
        it "StoreTest" $ do
          key' <- flip runZookeeperPool conn $ do
            insert val
          v <- flip runZookeeperPool conn $ do
            get key' :: ZookeeperT IO (Maybe Person)
          print $ show key'
          v `shouldBe` (Just val)
          v' <- flip runZookeeperPool conn $ do
            delete key'
            get key'
          v' `shouldBe` Nothing
