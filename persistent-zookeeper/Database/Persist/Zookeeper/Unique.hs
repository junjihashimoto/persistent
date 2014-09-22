{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Unique
       where

import Database.Persist
import Control.Applicative (Applicative)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store
import Database.Persist.Zookeeper.ZooUtil

instance (Applicative m, Functor m, MonadIO m, MonadBaseControl IO m) => PersistUnique (ZookeeperT m) where
    getBy uniqVal = do
      let key = uniqkey2key uniqVal
      val <- get key
      case val of
        Nothing -> return Nothing
        Just v -> return $ Just $ Entity key v

    deleteBy uniqVal = do
      let key = uniqkey2key uniqVal
      delete key
      
    insertUnique val = do
      mUniqVal <- val2uniqkey val
      case mUniqVal of
        Just uniqVal -> do
          let key@(Key (PersistText txt)) = uniqkey2key uniqVal
          execZookeeperT $ \zk -> do
            let dir = entity2path val
            r <- zCreate zk dir (T.unpack txt) (Just (entity2bin val)) []
            case r of
              Right _ -> return $ Right $ Just key
              Left Z.NodeExistsError -> return $ Right $ Nothing
              Left v -> return $ Left v
        Nothing -> do
          return $ Nothing

