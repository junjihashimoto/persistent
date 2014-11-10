{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Store (
  deleteRecursive
, BackendKey(..)
)where

import Database.Persist
import qualified Database.Persist.Sql as Sql
import Control.Exception
import Control.Applicative
import qualified Database.Zookeeper as Z
import Data.Monoid
import qualified Data.Text as T
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.ZooUtil
import Control.Monad
import Control.Monad.Reader
import qualified Data.Aeson as A


deleteRecursive :: (Monad m, MonadIO m) => String -> Action m ()
deleteRecursive dir = execZookeeper $ \zk -> zDeleteRecursive zk dir

instance Sql.PersistFieldSql (BackendKey Z.Zookeeper) where
    sqlType _ = Sql.SqlOther "doesn't make much sense for Zookeeper"

instance A.ToJSON (BackendKey Z.Zookeeper) where
    toJSON (ZooKey key) = A.String key

instance A.FromJSON (BackendKey Z.Zookeeper) where
    parseJSON (A.String key) = pure $ ZooKey key
    parseJSON _ = mzero


instance PersistStore Z.Zookeeper where
    newtype BackendKey Z.Zookeeper = ZooKey { unZooKey :: T.Text }
        deriving (Show, Read, Eq, Ord, PersistField)

    insert val = do
      let dir = entity2path val
      let path = dir <> "/"
      str <- execZookeeper $ \zk -> do
        zCreate zk dir path (Just (entity2bin val)) [Z.Sequence]
      return $ txtToKey $ T.pack $ str

    insertKey key val = do
      _ <- execZookeeper $ \zk -> do
        let dir = entity2path val
        zCreate zk dir (T.unpack (keyToTxt key)) (Just (entity2bin val)) []
      return ()

    repsert key val = do
      _ <- execZookeeper $ \zk -> do
        let dir = entity2path val
        zRepSert zk dir (T.unpack (keyToTxt key)) (Just (entity2bin val))
      return ()

    replace key val = do
      execZookeeper $ \zk -> do
        _ <- zReplace zk (T.unpack (keyToTxt key)) (Just (entity2bin val))
        return $ Right ()
      return ()

    delete key = do
      execZookeeper $ \zk -> do
        _ <- Z.delete zk (T.unpack (keyToTxt key)) Nothing
        return $ Right ()
      return ()

    get key = do
      r <- execZookeeper $ \zk -> do
        val <- Z.get zk (T.unpack (keyToTxt key)) Nothing
        return $ Right val
      case r of
        (Left Z.NoNodeError) ->
          return Nothing
        (Left v) ->
          fail $ show v
        (Right (Just str,_sta)) -> do
          return (bin2entity str)
        (Right (Nothing,_stat)) -> do
          fail $ "data is nothing"
      
    update key valList = do
      va <- get key
      case va of
        Nothing -> return ()
        Just v ->
          case updateEntity v valList of
            Right v' -> 
              replace key v'
            Left v' -> error $ show v'
