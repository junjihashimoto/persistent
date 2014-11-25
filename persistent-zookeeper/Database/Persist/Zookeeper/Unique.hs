{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Unique
       where

import Database.Persist
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store
import Database.Persist.Zookeeper.ZooUtil

instance PersistUnique Z.Zookeeper where
    getBy uniqVal = do
      let key = uniqkey2key uniqVal
      val <- get key
      case val of
        Nothing -> return Nothing
        Just v -> return $ Just $ Entity key v

    deleteBy uniqVal = do
      let key = uniqkey2key uniqVal
      delete key
      
