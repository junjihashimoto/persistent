{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Query
       where

import Database.Persist
import Control.Applicative (Applicative)
import Control.Monad
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store
import Database.Persist.Zookeeper.ZooUtil
import Data.Maybe
import Data.Conduit
import Data.Conduit.Lift



instance (Applicative m, Functor m, MonadIO m, MonadBaseControl IO m) => PersistQuery (ZookeeperT m) where
  update key valList = return ()
  updateGet key valLIst = return $ fromJust $ dummyFromKey key
  updateWhere filterList valList = return ()
  deleteWhere filterList = return ()
  selectSource filterList selectOpts = do
    let dir = entity2path $ dummyFromFList filterList
    -- str <- liftM  $ execZookeeperT $ \zk -> do
    --   Z.getChildren dir
    -- loop ["hoge"]
    return ()
    -- where
    --   loop [] = return ()
    --   loop (x:xs) = do
    --     yield $ (get (txtToKey x) :: ZookeeperT IO a)
    --     loop xs
    
  selectFirst filterList selectOpts = return Nothing
  selectKeys filterList selectOpts = return ()
  count filterList = return 0
  
